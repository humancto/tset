use crate::constants::HASH_SIZE;

pub type Hash = [u8; HASH_SIZE];

#[inline]
pub fn hash_bytes(data: &[u8]) -> Hash {
    *blake3::hash(data).as_bytes()
}

/// Balanced binary Merkle tree over leaves IN GIVEN ORDER (no sort).
/// Matches Python's `merkle_root()` (which `shard_merkle_root` then sorts
/// before calling).
pub fn merkle_root_unsorted(leaves: &[Hash]) -> Hash {
    if leaves.is_empty() {
        return [0u8; HASH_SIZE];
    }
    let mut level: Vec<Hash> = leaves
        .iter()
        .map(|h| {
            let mut buf = Vec::with_capacity(1 + HASH_SIZE);
            buf.push(0x00);
            buf.extend_from_slice(h);
            hash_bytes(&buf)
        })
        .collect();
    while level.len() > 1 {
        if level.len() % 2 == 1 {
            level.push(*level.last().unwrap());
        }
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks_exact(2) {
            let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
            buf.push(0x01);
            buf.extend_from_slice(&pair[0]);
            buf.extend_from_slice(&pair[1]);
            next.push(hash_bytes(&buf));
        }
        level = next;
    }
    level[0]
}

/// Per SPEC §6: balanced binary Merkle tree over **sorted** doc hashes.
/// - empty input → 32 zero bytes
/// - leaf node     = hash(0x00 || leaf)
/// - internal node = hash(0x01 || left || right)
/// - odd levels duplicate the last hash (Bitcoin-style)
///
/// Sorting is what makes the root order-independent across writer/reader
/// (writer sees insertion order; reader sees JSON-sort-keys order).
pub fn shard_merkle_root(leaves: &[Hash]) -> Hash {
    if leaves.is_empty() {
        return [0u8; HASH_SIZE];
    }
    let mut sorted: Vec<Hash> = leaves.to_vec();
    sorted.sort();
    let mut level: Vec<Hash> = sorted
        .iter()
        .map(|h| {
            let mut buf = Vec::with_capacity(1 + HASH_SIZE);
            buf.push(0x00);
            buf.extend_from_slice(h);
            hash_bytes(&buf)
        })
        .collect();

    while level.len() > 1 {
        if level.len() % 2 == 1 {
            level.push(*level.last().unwrap());
        }
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks_exact(2) {
            let mut buf = Vec::with_capacity(1 + 2 * HASH_SIZE);
            buf.push(0x01);
            buf.extend_from_slice(&pair[0]);
            buf.extend_from_slice(&pair[1]);
            next.push(hash_bytes(&buf));
        }
        level = next;
    }
    level[0]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_root_is_zero() {
        assert_eq!(shard_merkle_root(&[]), [0u8; HASH_SIZE]);
    }

    #[test]
    fn single_leaf_is_hashed_with_domain_byte() {
        let leaf = hash_bytes(b"hello");
        let mut expect = vec![0x00];
        expect.extend_from_slice(&leaf);
        let expected = hash_bytes(&expect);
        assert_eq!(shard_merkle_root(&[leaf]), expected);
    }

    #[test]
    fn deterministic_across_runs() {
        let leaves: Vec<Hash> = (0..7)
            .map(|i| hash_bytes(format!("doc-{i}").as_bytes()))
            .collect();
        assert_eq!(shard_merkle_root(&leaves), shard_merkle_root(&leaves));
    }
}
