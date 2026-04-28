//! Sparse Merkle Tree over BLAKE3 document hashes.
//!
//! Wire format mirrors the Python reference impl (`python/tset/smt.py`):
//!   - `LEAF_PREFIX     = 0x10`
//!   - `INTERNAL_PREFIX = 0x11`
//!   - `ABSENT_LEAF  = BLAKE3(0x10 || 0x00)`
//!   - `PRESENT_LEAF = BLAKE3(0x10 || 0x01)`
//!   - internal node hash = `BLAKE3(0x11 || left || right)`
//!
//! Bits are MSB-first across the 256-bit key, matching `_bit(k, i)` in
//! Python.
//!
//! Per RFC §10 items 14-18, the on-disk SMT layout is "design under
//! review"; the manifest stores `smt_root` plus a serialized
//! `smt_present_keys` list under `smt_version` so the encoding can change
//! later without breaking older readers.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use crate::constants::HASH_SIZE;
use crate::hashing::{hash_bytes, Hash};

pub const SMT_DEPTH: usize = 256;
pub const LEAF_PREFIX: u8 = 0x10;
pub const INTERNAL_PREFIX: u8 = 0x11;

fn empty_levels() -> &'static [Hash; SMT_DEPTH + 1] {
    static EMPTY: OnceLock<Box<[Hash; SMT_DEPTH + 1]>> = OnceLock::new();
    EMPTY.get_or_init(|| {
        let mut levels: Vec<Hash> = Vec::with_capacity(SMT_DEPTH + 1);
        let absent = absent_leaf();
        levels.push(absent);
        let mut cur = absent;
        for _ in 0..SMT_DEPTH {
            let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
            buf.push(INTERNAL_PREFIX);
            buf.extend_from_slice(&cur);
            buf.extend_from_slice(&cur);
            cur = hash_bytes(&buf);
            levels.push(cur);
        }
        let mut arr = [[0u8; HASH_SIZE]; SMT_DEPTH + 1];
        for (i, h) in levels.into_iter().enumerate() {
            arr[i] = h;
        }
        Box::new(arr)
    })
}

pub fn absent_leaf() -> Hash {
    hash_bytes(&[LEAF_PREFIX, 0x00])
}

pub fn present_leaf() -> Hash {
    hash_bytes(&[LEAF_PREFIX, 0x01])
}

pub fn empty_root() -> Hash {
    empty_levels()[SMT_DEPTH]
}

#[inline]
fn bit_msb(key: &Hash, i: usize) -> u8 {
    (key[i >> 3] >> (7 - (i & 7))) & 1
}

/// Recompute the root from a leaf hash + top-down siblings (`siblings[0]`
/// is the sibling of the root's child, `siblings[SMT_DEPTH-1]` is the
/// leaf's sibling).
pub fn verify_path(key: &Hash, leaf_hash: Hash, siblings: &[Hash]) -> Option<Hash> {
    if siblings.len() != SMT_DEPTH {
        return None;
    }
    let mut node = leaf_hash;
    for level in 0..SMT_DEPTH {
        let depth = SMT_DEPTH - 1 - level;
        let sibling = siblings[depth];
        let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
        buf.push(INTERNAL_PREFIX);
        if bit_msb(key, depth) == 0 {
            buf.extend_from_slice(&node);
            buf.extend_from_slice(&sibling);
        } else {
            buf.extend_from_slice(&sibling);
            buf.extend_from_slice(&node);
        }
        node = hash_bytes(&buf);
    }
    Some(node)
}

#[derive(Debug, Clone)]
pub enum Proof {
    Inclusion(InclusionProof),
    NonInclusion(NonInclusionProof),
}

#[derive(Debug, Clone)]
pub struct InclusionProof {
    pub key: Hash,
    pub siblings: Vec<Hash>,
}

impl InclusionProof {
    pub fn verify(&self, expected_root: &Hash) -> bool {
        verify_path(&self.key, present_leaf(), &self.siblings).is_some_and(|r| &r == expected_root)
    }
}

#[derive(Debug, Clone)]
pub struct NonInclusionProof {
    pub key: Hash,
    pub siblings: Vec<Hash>,
}

impl NonInclusionProof {
    pub fn verify(&self, expected_root: &Hash) -> bool {
        verify_path(&self.key, absent_leaf(), &self.siblings).is_some_and(|r| &r == expected_root)
    }
}

/// In-memory SMT. Path-only representation: only paths leading to
/// present leaves are materialized; everything else uses the
/// precomputed `EMPTY[d]` hashes.
#[derive(Debug, Default)]
pub struct SparseMerkleTree {
    present: BTreeSet<Hash>,
    root: Option<Box<Internal>>,
}

#[derive(Debug, Default)]
struct Internal {
    left: Option<Child>,
    right: Option<Child>,
}

#[derive(Debug)]
enum Child {
    Internal(Box<Internal>),
    Leaf,
}

impl SparseMerkleTree {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.present.len()
    }

    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    pub fn has(&self, key: &Hash) -> bool {
        self.present.contains(key)
    }

    pub fn present_keys(&self) -> Vec<Hash> {
        self.present.iter().copied().collect()
    }

    pub fn insert(&mut self, key: Hash) {
        if !self.present.insert(key) {
            return;
        }
        if self.root.is_none() {
            self.root = Some(Box::new(Internal::default()));
        }
        let mut node: &mut Internal = self.root.as_mut().unwrap();
        for depth in 0..(SMT_DEPTH - 1) {
            let bit = bit_msb(&key, depth);
            if bit == 0 {
                if node.left.is_none() {
                    node.left = Some(Child::Internal(Box::default()));
                }
                node = match node.left.as_mut().unwrap() {
                    Child::Internal(b) => b.as_mut(),
                    Child::Leaf => unreachable!("leaf can only appear at SMT_DEPTH-1"),
                };
            } else {
                if node.right.is_none() {
                    node.right = Some(Child::Internal(Box::default()));
                }
                node = match node.right.as_mut().unwrap() {
                    Child::Internal(b) => b.as_mut(),
                    Child::Leaf => unreachable!("leaf can only appear at SMT_DEPTH-1"),
                };
            }
        }
        let last_bit = bit_msb(&key, SMT_DEPTH - 1);
        if last_bit == 0 {
            node.left = Some(Child::Leaf);
        } else {
            node.right = Some(Child::Leaf);
        }
    }

    fn hash_subtree(child: Option<&Child>, depth: usize) -> Hash {
        match child {
            None => empty_levels()[SMT_DEPTH - depth],
            Some(Child::Leaf) => present_leaf(),
            Some(Child::Internal(node)) => {
                let left = Self::hash_subtree(node.left.as_ref(), depth + 1);
                let right = Self::hash_subtree(node.right.as_ref(), depth + 1);
                let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
                buf.push(INTERNAL_PREFIX);
                buf.extend_from_slice(&left);
                buf.extend_from_slice(&right);
                hash_bytes(&buf)
            }
        }
    }

    pub fn root(&self) -> Hash {
        match &self.root {
            None => empty_root(),
            Some(root) => {
                let l = Self::hash_subtree(root.left.as_ref(), 1);
                let r = Self::hash_subtree(root.right.as_ref(), 1);
                let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
                buf.push(INTERNAL_PREFIX);
                buf.extend_from_slice(&l);
                buf.extend_from_slice(&r);
                hash_bytes(&buf)
            }
        }
    }

    pub fn prove(&self, key: &Hash) -> Proof {
        let mut siblings: Vec<Hash> = Vec::with_capacity(SMT_DEPTH);
        let mut node: Option<&Child> = match &self.root {
            None => None,
            Some(_root) => {
                let bit = bit_msb(key, 0);
                if bit == 0 {
                    siblings.push(Self::hash_subtree(_root.right.as_ref(), 1));
                    _root.left.as_ref()
                } else {
                    siblings.push(Self::hash_subtree(_root.left.as_ref(), 1));
                    _root.right.as_ref()
                }
            }
        };
        if self.root.is_none() {
            siblings.push(empty_levels()[SMT_DEPTH - 1]);
        }

        for depth in 1..SMT_DEPTH {
            let bit = bit_msb(key, depth);
            match node {
                None => {
                    siblings.push(empty_levels()[SMT_DEPTH - depth - 1]);
                }
                Some(Child::Leaf) => {
                    siblings.push(empty_levels()[SMT_DEPTH - depth - 1]);
                    node = None;
                }
                Some(Child::Internal(inner)) => {
                    if bit == 0 {
                        siblings.push(Self::hash_subtree(inner.right.as_ref(), depth + 1));
                        node = inner.left.as_ref();
                    } else {
                        siblings.push(Self::hash_subtree(inner.left.as_ref(), depth + 1));
                        node = inner.right.as_ref();
                    }
                }
            }
        }

        if self.present.contains(key) {
            Proof::Inclusion(InclusionProof {
                key: *key,
                siblings,
            })
        } else {
            Proof::NonInclusion(NonInclusionProof {
                key: *key,
                siblings,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_root_matches_iterated_construction() {
        // Reconstruct the empty tree root by climbing levels manually.
        let mut cur = absent_leaf();
        for _ in 0..SMT_DEPTH {
            let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
            buf.push(INTERNAL_PREFIX);
            buf.extend_from_slice(&cur);
            buf.extend_from_slice(&cur);
            cur = hash_bytes(&buf);
        }
        assert_eq!(cur, empty_root());
    }

    #[test]
    fn insert_and_prove_inclusion() {
        let mut t = SparseMerkleTree::new();
        let key = hash_bytes(b"doc-1");
        t.insert(key);
        match t.prove(&key) {
            Proof::Inclusion(p) => assert!(p.verify(&t.root())),
            _ => panic!("expected inclusion proof"),
        }
    }

    #[test]
    fn prove_non_inclusion_for_absent_key() {
        let mut t = SparseMerkleTree::new();
        t.insert(hash_bytes(b"doc-1"));
        let absent = hash_bytes(b"doc-missing");
        match t.prove(&absent) {
            Proof::NonInclusion(p) => assert!(p.verify(&t.root())),
            _ => panic!("expected non-inclusion proof"),
        }
    }

    #[test]
    fn root_changes_after_insert() {
        let mut t = SparseMerkleTree::new();
        let r0 = t.root();
        t.insert(hash_bytes(b"x"));
        let r1 = t.root();
        assert_ne!(r0, r1);
    }
}
