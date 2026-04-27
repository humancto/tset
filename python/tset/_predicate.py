"""Tiny predicate compiler for `MetadataColumns.filter_sql_like`.

Supports a deliberately small subset of SQL-like predicates over column rows:

    expr      := or_expr
    or_expr   := and_expr ( OR and_expr )*
    and_expr  := atom    ( AND atom )*
    atom      := '(' expr ')' | comparison
    comparison:= IDENT (= | != | > | < | >= | <=) literal
               | IDENT IN (literal, ...)
               | IDENT LIKE 'string-with-%-wildcards'

This is not a general SQL parser. It exists to make benchmark D's query 1
("source_url LIKE '%nyt.com%'") and query 2 ("quality_score < 0.3")
expressible without arbitrary `eval`.
"""

from __future__ import annotations

import re
import string
from typing import Any, Callable


_TOKEN_RE = re.compile(
    r"""
    \s*(
        '(?:[^'\\]|\\.)*' |
        "(?:[^"\\]|\\.)*" |
        [A-Za-z_][A-Za-z0-9_]* |
        -?\d+(?:\.\d+)? |
        \(|\)|,|>=|<=|!=|=|>|<
    )
    """,
    re.VERBOSE,
)
_KEYWORDS = {
    "AND", "OR", "NOT", "IN", "IS", "LIKE", "BETWEEN",
    "TRUE", "FALSE", "NULL",
}


def _tokenize(expr: str) -> list[str]:
    pos = 0
    out = []
    while pos < len(expr):
        m = _TOKEN_RE.match(expr, pos)
        if not m:
            raise ValueError(f"unexpected char at offset {pos}: {expr[pos:pos+10]!r}")
        out.append(m.group(1))
        pos = m.end()
    return out


def _is_ident(tok: str) -> bool:
    if not tok:
        return False
    if tok[0] not in string.ascii_letters + "_":
        return False
    return all(c.isalnum() or c == "_" for c in tok) and tok.upper() not in _KEYWORDS


def _parse_literal(tok: str) -> Any:
    if not tok:
        raise ValueError("empty literal")
    if tok[0] in "\"'":
        return tok[1:-1].encode().decode("unicode_escape")
    upper = tok.upper()
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False
    if upper == "NULL":
        return None
    try:
        if "." in tok:
            return float(tok)
        return int(tok)
    except ValueError as e:
        raise ValueError(f"bad literal: {tok!r}") from e


def _like_to_regex(pattern: str) -> re.Pattern:
    out = ["^"]
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    out.append("$")
    return re.compile("".join(out), re.DOTALL)


class _Parser:
    def __init__(self, tokens: list[str], types: dict[str, str]):
        self.tokens = tokens
        self.pos = 0
        self.types = types

    def peek(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def eat(self) -> str:
        t = self.tokens[self.pos]
        self.pos += 1
        return t

    def expect_kw(self, kw: str) -> None:
        t = self.eat()
        if t.upper() != kw.upper():
            raise ValueError(f"expected {kw}, got {t!r}")

    def parse(self) -> Callable[[dict], bool]:
        pred = self.parse_or()
        if self.pos != len(self.tokens):
            raise ValueError(f"trailing tokens: {self.tokens[self.pos:]!r}")
        return pred

    def parse_or(self) -> Callable[[dict], bool]:
        left = self.parse_and()
        while self.peek() and self.peek().upper() == "OR":
            self.eat()
            right = self.parse_and()
            l, r = left, right
            left = lambda row, l=l, r=r: l(row) or r(row)
        return left

    def parse_and(self) -> Callable[[dict], bool]:
        left = self.parse_atom()
        while self.peek() and self.peek().upper() == "AND":
            self.eat()
            right = self.parse_atom()
            l, r = left, right
            left = lambda row, l=l, r=r: l(row) and r(row)
        return left

    def parse_atom(self) -> Callable[[dict], bool]:
        t = self.peek()
        if t and t.upper() == "NOT":
            self.eat()
            inner = self.parse_atom()
            return lambda row, n=inner: not n(row)
        if t == "(":
            self.eat()
            inner = self.parse_or()
            if self.eat() != ")":
                raise ValueError("missing )")
            return inner
        return self.parse_comparison()

    def parse_comparison(self) -> Callable[[dict], bool]:
        ident = self.eat()
        if not _is_ident(ident):
            raise ValueError(f"expected identifier, got {ident!r}")
        op = self.eat()
        op_upper = op.upper()
        if op_upper == "IS":
            nxt = self.eat()
            nxt_upper = nxt.upper()
            negated = False
            if nxt_upper == "NOT":
                kw = self.eat()
                if kw.upper() != "NULL":
                    raise ValueError("expected NULL after IS NOT")
                negated = True
            elif nxt_upper != "NULL":
                raise ValueError("expected NULL or NOT NULL after IS")
            if negated:
                return lambda row, c=ident: row.get(c) is not None
            return lambda row, c=ident: row.get(c) is None
        if op_upper == "BETWEEN":
            low = _parse_literal(self.eat())
            and_kw = self.eat()
            if and_kw.upper() != "AND":
                raise ValueError("expected AND in BETWEEN")
            high = _parse_literal(self.eat())
            return lambda row, c=ident, lo=low, hi=high: (
                row.get(c) is not None and lo <= row.get(c) <= hi
            )
        if op_upper == "IN":
            if self.eat() != "(":
                raise ValueError("expected ( after IN")
            values = []
            while True:
                values.append(_parse_literal(self.eat()))
                t = self.eat()
                if t == ")":
                    break
                if t != ",":
                    raise ValueError(f"expected , or ) in IN list, got {t!r}")
            value_set = set(values)
            return lambda row, c=ident, vs=value_set: row.get(c) in vs
        if op_upper == "LIKE":
            pat_tok = self.eat()
            if pat_tok[0] not in "\"'":
                raise ValueError("LIKE expects a string literal")
            pattern = _like_to_regex(pat_tok[1:-1])
            return lambda row, c=ident, p=pattern: row.get(c) is not None and bool(p.match(str(row.get(c))))
        rhs = _parse_literal(self.eat())
        if op == "=":
            return lambda row, c=ident, v=rhs: row.get(c) == v
        if op == "!=":
            return lambda row, c=ident, v=rhs: row.get(c) != v
        if op == ">":
            return lambda row, c=ident, v=rhs: row.get(c) is not None and row.get(c) > v
        if op == "<":
            return lambda row, c=ident, v=rhs: row.get(c) is not None and row.get(c) < v
        if op == ">=":
            return lambda row, c=ident, v=rhs: row.get(c) is not None and row.get(c) >= v
        if op == "<=":
            return lambda row, c=ident, v=rhs: row.get(c) is not None and row.get(c) <= v
        raise ValueError(f"unknown operator {op!r}")


def compile_predicate(expr: str, types: dict[str, str]) -> Callable[[dict], bool]:
    return _Parser(_tokenize(expr), types).parse()
