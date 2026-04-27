//! Sparse Merkle Tree — verification side only in this crate revision.
//! Full insert/proof generation lives in the writer crate (TBD) and
//! re-uses these constants and the `_verify_path` arithmetic.

use crate::constants::HASH_SIZE;
use crate::hashing::hash_bytes;

pub const SMT_DEPTH: usize = 256;
pub const PRESENT_LEAF: u8 = 0x01;
pub const ABSENT_LEAF: u8 = 0x00;

pub fn empty_subtree_root(depth: usize) -> [u8; HASH_SIZE] {
    let mut current = hash_bytes(&[ABSENT_LEAF]);
    for _ in 0..depth {
        let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
        buf.push(0x01);
        buf.extend_from_slice(&current);
        buf.extend_from_slice(&current);
        current = hash_bytes(&buf);
    }
    current
}

#[inline]
fn bit_msb_first(key: &[u8; HASH_SIZE], depth: usize) -> u8 {
    // depth 0 = most significant bit of byte 0
    let byte = key[depth / 8];
    (byte >> (7 - (depth % 8))) & 1
}

pub fn verify_path(
    key: &[u8; HASH_SIZE],
    leaf_present: bool,
    siblings: &[[u8; HASH_SIZE]],
    expected_root: &[u8; HASH_SIZE],
) -> bool {
    if siblings.len() != SMT_DEPTH {
        return false;
    }
    let leaf_byte = if leaf_present { PRESENT_LEAF } else { ABSENT_LEAF };
    let mut current = hash_bytes(&[leaf_byte]);
    // Siblings are listed top-down (root → leaf in Python `prove`); we walk
    // bottom-up here, so reverse them.
    for depth in (0..SMT_DEPTH).rev() {
        let bit = bit_msb_first(key, depth);
        let sib = siblings[depth];
        let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
        buf.push(0x01);
        if bit == 0 {
            buf.extend_from_slice(&current);
            buf.extend_from_slice(&sib);
        } else {
            buf.extend_from_slice(&sib);
            buf.extend_from_slice(&current);
        }
        current = hash_bytes(&buf);
    }
    &current == expected_root
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tree_root_matches_iterated_construction() {
        let root = empty_subtree_root(SMT_DEPTH);
        // Verifying any absent key against the empty root should succeed
        // when the full sibling chain is the matching empty subtree levels.
        let key = [0u8; HASH_SIZE];
        let mut sibs: Vec<[u8; HASH_SIZE]> = Vec::with_capacity(SMT_DEPTH);
        let mut acc = hash_bytes(&[ABSENT_LEAF]);
        for _ in 0..SMT_DEPTH {
            sibs.push(acc);
            let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
            buf.push(0x01);
            buf.extend_from_slice(&acc);
            buf.extend_from_slice(&acc);
            acc = hash_bytes(&buf);
        }
        // Python prove() emits siblings top-down (root→leaf); our test
        // built bottom-up so reverse for consistency with verify_path's
        // expected ordering.
        sibs.reverse();
        assert!(verify_path(&key, false, &sibs, &root));
    }
}
