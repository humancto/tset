//! Per-document metadata columns + the tiny SQL-like predicate compiler
//! used by `MetadataColumns::filter_sql_like`.
//!
//! Wire format mirrors Python (`python/tset/columns.py`):
//!
//! ```json
//! {
//!   "row_count": <u64>,
//!   "types":   { "<col>": "<int|float|bool|string|categorical>", ... },
//!   "columns": { "<col>": [ <values...> ], ... }
//! }
//! ```
//!
//! Values are serde_json `Value`s — None preserves as `null`.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::error::{TsetError, TsetResult};

#[derive(Debug, Default, Clone)]
pub struct MetadataColumns {
    columns: BTreeMap<String, Vec<Value>>,
    types: BTreeMap<String, String>,
    row_count: u64,
    insertion_order: Vec<String>,
}

impl MetadataColumns {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn names(&self) -> Vec<&str> {
        self.insertion_order.iter().map(String::as_str).collect()
    }

    pub fn column(&self, name: &str) -> Option<&[Value]> {
        self.columns.get(name).map(|v| v.as_slice())
    }

    pub fn column_type(&self, name: &str) -> Option<&str> {
        self.types.get(name).map(String::as_str)
    }

    pub fn declare(&mut self, name: &str, logical_type: &str) -> TsetResult<()> {
        if !matches!(
            logical_type,
            "string" | "categorical" | "int" | "float" | "bool"
        ) {
            return Err(TsetError::BadManifest("unknown column logical type"));
        }
        if !self.columns.contains_key(name) {
            self.columns.insert(
                name.to_string(),
                vec![Value::Null; self.row_count as usize],
            );
            self.types.insert(name.to_string(), logical_type.to_string());
            self.insertion_order.push(name.to_string());
        }
        Ok(())
    }

    pub fn add_row(&mut self, values: &Map<String, Value>) {
        for (k, v) in values {
            if !self.columns.contains_key(k) {
                let _ = self.declare(k, infer_type(v));
            }
        }
        for (col, vals) in self.columns.iter_mut() {
            vals.push(values.get(col).cloned().unwrap_or(Value::Null));
        }
        self.row_count += 1;
    }

    pub fn filter_sql_like(&self, expr: &str) -> TsetResult<Vec<usize>> {
        let pred = compile_predicate(expr, &self.types)?;
        let mut out = Vec::new();
        for i in 0..self.row_count as usize {
            if pred.eval(&|col: &str| {
                self.columns
                    .get(col)
                    .map(|v| v[i].clone())
                    .unwrap_or(Value::Null)
            }) {
                out.push(i);
            }
        }
        Ok(out)
    }

    pub fn to_json(&self) -> Value {
        let mut cols = Map::new();
        for name in &self.insertion_order {
            cols.insert(
                name.clone(),
                Value::Array(self.columns.get(name).cloned().unwrap_or_default()),
            );
        }
        let mut types = Map::new();
        for (k, v) in &self.types {
            types.insert(k.clone(), Value::String(v.clone()));
        }
        serde_json::json!({
            "row_count": self.row_count,
            "types": types,
            "columns": cols,
        })
    }
}

fn infer_type(v: &Value) -> &'static str {
    match v {
        Value::Bool(_) => "bool",
        Value::Number(n) if n.is_i64() || n.is_u64() => "int",
        Value::Number(_) => "float",
        _ => "string",
    }
}

// --- predicate compiler ---

#[derive(Debug)]
enum Op {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

#[derive(Debug)]
enum Atom {
    Cmp { col: String, op: Op, rhs: Value },
    In { col: String, set: Vec<Value> },
    Like { col: String, pattern: regex::Regex },
    Between { col: String, low: Value, high: Value },
    IsNull { col: String, negated: bool },
}

#[derive(Debug)]
enum Node {
    Atom(Atom),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    Not(Box<Node>),
}

pub struct Predicate {
    root: Node,
}

impl Predicate {
    pub fn eval(&self, getter: &dyn Fn(&str) -> Value) -> bool {
        eval_node(&self.root, getter)
    }
}

fn eval_node(node: &Node, getter: &dyn Fn(&str) -> Value) -> bool {
    match node {
        Node::And(l, r) => eval_node(l, getter) && eval_node(r, getter),
        Node::Or(l, r) => eval_node(l, getter) || eval_node(r, getter),
        Node::Not(n) => !eval_node(n, getter),
        Node::Atom(a) => eval_atom(a, getter),
    }
}

fn eval_atom(atom: &Atom, getter: &dyn Fn(&str) -> Value) -> bool {
    match atom {
        Atom::In { col, set } => {
            let v = getter(col);
            set.iter().any(|x| values_equal(x, &v))
        }
        Atom::Like { col, pattern } => {
            let v = getter(col);
            match v.as_str() {
                Some(s) => pattern.is_match(s),
                None => false,
            }
        }
        Atom::Between { col, low, high } => {
            let v = getter(col);
            (compare_gt(&v, low) || values_equal(&v, low))
                && (compare_lt(&v, high) || values_equal(&v, high))
        }
        Atom::IsNull { col, negated } => {
            let is_null = matches!(getter(col), Value::Null);
            if *negated { !is_null } else { is_null }
        }
        Atom::Cmp { col, op, rhs } => {
            let v = getter(col);
            match op {
                Op::Eq => values_equal(&v, rhs),
                Op::Neq => !values_equal(&v, rhs),
                Op::Gt => compare_gt(&v, rhs),
                Op::Lt => compare_lt(&v, rhs),
                Op::Gte => compare_gt(&v, rhs) || values_equal(&v, rhs),
                Op::Lte => compare_lt(&v, rhs) || values_equal(&v, rhs),
            }
        }
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().unwrap_or(f64::NAN) == y.as_f64().unwrap_or(f64::NAN)
        }
        _ => a == b,
    }
}

fn compare_gt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().unwrap_or(f64::NAN) > y.as_f64().unwrap_or(f64::NAN)
        }
        (Value::String(x), Value::String(y)) => x > y,
        (Value::Bool(x), Value::Bool(y)) => x & !y, // matches Python's True > False
        _ => false,
    }
}

fn compare_lt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().unwrap_or(f64::NAN) < y.as_f64().unwrap_or(f64::NAN)
        }
        (Value::String(x), Value::String(y)) => x < y,
        (Value::Bool(x), Value::Bool(y)) => !x & y,
        _ => false,
    }
}

pub fn compile_predicate(
    expr: &str,
    _types: &BTreeMap<String, String>,
) -> TsetResult<Predicate> {
    let tokens = tokenize(expr)?;
    let mut parser = Parser { tokens, pos: 0 };
    let root = parser.parse_or()?;
    if parser.pos != parser.tokens.len() {
        return Err(TsetError::BadManifest("trailing tokens in predicate"));
    }
    Ok(Predicate { root })
}

fn tokenize(s: &str) -> TsetResult<Vec<String>> {
    let mut out = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '\'' || c == '"' {
            let quote = c;
            let start = i;
            i += 1;
            while i < chars.len() && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if i >= chars.len() {
                return Err(TsetError::BadManifest("unterminated string literal"));
            }
            i += 1;
            out.push(chars[start..i].iter().collect());
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            out.push(chars[start..i].iter().collect());
            continue;
        }
        if c == '-' || c.is_ascii_digit() {
            let start = i;
            if c == '-' {
                i += 1;
            }
            let mut saw_digit = false;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
                saw_digit = true;
            }
            if i < chars.len() && chars[i] == '.' {
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                    saw_digit = true;
                }
            }
            if !saw_digit {
                return Err(TsetError::BadManifest("bare '-' in predicate"));
            }
            out.push(chars[start..i].iter().collect());
            continue;
        }
        match c {
            '(' | ')' | ',' => {
                out.push(c.to_string());
                i += 1;
            }
            '>' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                out.push(">=".to_string());
                i += 2;
            }
            '<' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                out.push("<=".to_string());
                i += 2;
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                out.push("!=".to_string());
                i += 2;
            }
            '=' | '>' | '<' => {
                out.push(c.to_string());
                i += 1;
            }
            _ => {
                return Err(TsetError::BadManifest("unexpected char in predicate"));
            }
        }
    }
    Ok(out)
}

struct Parser {
    tokens: Vec<String>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(String::as_str)
    }
    fn eat(&mut self) -> TsetResult<String> {
        if self.pos >= self.tokens.len() {
            return Err(TsetError::BadManifest("unexpected end of predicate"));
        }
        let s = self.tokens[self.pos].clone();
        self.pos += 1;
        Ok(s)
    }
    fn parse_or(&mut self) -> TsetResult<Node> {
        let mut left = self.parse_and()?;
        while let Some(t) = self.peek() {
            if t.eq_ignore_ascii_case("OR") {
                self.eat()?;
                let right = self.parse_and()?;
                left = Node::Or(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }
    fn parse_and(&mut self) -> TsetResult<Node> {
        let mut left = self.parse_atom()?;
        while let Some(t) = self.peek() {
            if t.eq_ignore_ascii_case("AND") {
                self.eat()?;
                let right = self.parse_atom()?;
                left = Node::And(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }
    fn parse_atom(&mut self) -> TsetResult<Node> {
        // NOT <atom>
        if let Some(t) = self.peek() {
            if t.eq_ignore_ascii_case("NOT") {
                self.eat()?;
                let inner = self.parse_atom()?;
                return Ok(Node::Not(Box::new(inner)));
            }
        }
        if self.peek() == Some("(") {
            self.eat()?;
            let inner = self.parse_or()?;
            if self.eat()? != ")" {
                return Err(TsetError::BadManifest("missing )"));
            }
            return Ok(inner);
        }
        let ident = self.eat()?;
        if !is_ident(&ident) {
            return Err(TsetError::BadManifest("expected identifier"));
        }
        let op = self.eat()?;
        let op_upper = op.to_ascii_uppercase();
        // <ident> IS [NOT] NULL
        if op_upper == "IS" {
            let next = self.eat()?;
            let next_upper = next.to_ascii_uppercase();
            let negated = if next_upper == "NOT" {
                let null = self.eat()?;
                if !null.eq_ignore_ascii_case("NULL") {
                    return Err(TsetError::BadManifest("expected NULL after IS NOT"));
                }
                true
            } else if next_upper == "NULL" {
                false
            } else {
                return Err(TsetError::BadManifest("expected NULL or NOT NULL after IS"));
            };
            return Ok(Node::Atom(Atom::IsNull { col: ident, negated }));
        }
        // <ident> BETWEEN <lit> AND <lit>
        if op_upper == "BETWEEN" {
            let low = parse_literal(&self.eat()?)?;
            let and_kw = self.eat()?;
            if !and_kw.eq_ignore_ascii_case("AND") {
                return Err(TsetError::BadManifest("expected AND in BETWEEN"));
            }
            let high = parse_literal(&self.eat()?)?;
            return Ok(Node::Atom(Atom::Between { col: ident, low, high }));
        }
        if op_upper == "IN" {
            if self.eat()? != "(" {
                return Err(TsetError::BadManifest("expected ( after IN"));
            }
            let mut values = Vec::new();
            loop {
                let lit = self.eat()?;
                values.push(parse_literal(&lit)?);
                let nxt = self.eat()?;
                if nxt == ")" {
                    break;
                }
                if nxt != "," {
                    return Err(TsetError::BadManifest("expected , or ) in IN list"));
                }
            }
            return Ok(Node::Atom(Atom::In { col: ident, set: values }));
        }
        if op_upper == "LIKE" {
            let pat = self.eat()?;
            if !pat.starts_with('\'') && !pat.starts_with('"') {
                return Err(TsetError::BadManifest("LIKE expects a string literal"));
            }
            let inner = &pat[1..pat.len() - 1];
            let mut re = String::from("^");
            for ch in inner.chars() {
                match ch {
                    '%' => re.push_str(".*"),
                    '_' => re.push('.'),
                    c => re.push_str(&regex::escape(&c.to_string())),
                }
            }
            re.push('$');
            let pattern = regex::Regex::new(&re)
                .map_err(|_| TsetError::BadManifest("invalid LIKE pattern"))?;
            return Ok(Node::Atom(Atom::Like { col: ident, pattern }));
        }
        let rhs_tok = self.eat()?;
        let rhs = parse_literal(&rhs_tok)?;
        let op_kind = match op.as_str() {
            "=" => Op::Eq,
            "!=" => Op::Neq,
            ">" => Op::Gt,
            "<" => Op::Lt,
            ">=" => Op::Gte,
            "<=" => Op::Lte,
            _ => return Err(TsetError::BadManifest("unknown operator")),
        };
        Ok(Node::Atom(Atom::Cmp { col: ident, op: op_kind, rhs }))
    }
}

fn is_ident(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let upper = s.to_ascii_uppercase();
    if matches!(
        upper.as_str(),
        "AND" | "OR" | "NOT" | "IN" | "IS" | "LIKE" | "BETWEEN"
            | "TRUE" | "FALSE" | "NULL"
    ) {
        return false;
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn parse_literal(tok: &str) -> TsetResult<Value> {
    if tok.is_empty() {
        return Err(TsetError::BadManifest("empty literal"));
    }
    let first = tok.chars().next().unwrap();
    if first == '\'' || first == '"' {
        let inner = &tok[1..tok.len() - 1];
        // unescape \\ \n \t etc minimally — match Python's unicode_escape
        let mut out = String::new();
        let mut chars = inner.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some('r') => out.push('\r'),
                    Some('\\') => out.push('\\'),
                    Some('\'') => out.push('\''),
                    Some('"') => out.push('"'),
                    Some(c) => out.push(c),
                    None => out.push('\\'),
                }
            } else {
                out.push(c);
            }
        }
        return Ok(Value::String(out));
    }
    let upper = tok.to_ascii_uppercase();
    if upper == "TRUE" {
        return Ok(Value::Bool(true));
    }
    if upper == "FALSE" {
        return Ok(Value::Bool(false));
    }
    if upper == "NULL" {
        return Ok(Value::Null);
    }
    if tok.contains('.') {
        let f: f64 = tok
            .parse()
            .map_err(|_| TsetError::BadManifest("bad float literal"))?;
        return Ok(serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null));
    }
    let n: i64 = tok
        .parse()
        .map_err(|_| TsetError::BadManifest("bad int literal"))?;
    Ok(Value::Number(serde_json::Number::from(n)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make() -> MetadataColumns {
        let mut c = MetadataColumns::new();
        for v in [
            json!({"a": 1, "b": "x"}),
            json!({"a": 2, "b": "y"}),
            json!({"a": 3, "b": "z"}),
            json!({"a": 4, "b": "y"}),
        ] {
            c.add_row(v.as_object().unwrap());
        }
        c
    }

    #[test]
    fn filter_basic_eq() {
        let c = make();
        assert_eq!(c.filter_sql_like("a = 1").unwrap(), vec![0]);
        assert_eq!(c.filter_sql_like("b = 'y'").unwrap(), vec![1, 3]);
    }

    #[test]
    fn filter_compound_paren() {
        let c = make();
        assert_eq!(
            c.filter_sql_like("(a > 1 AND a <= 4) OR b = 'z'").unwrap(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn filter_in_and_like() {
        let c = make();
        assert_eq!(c.filter_sql_like("b IN ('x', 'y')").unwrap(), vec![0, 1, 3]);
        assert_eq!(c.filter_sql_like("b LIKE '%y%'").unwrap(), vec![1, 3]);
    }
}
