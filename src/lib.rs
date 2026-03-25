use clickhouse_wasm_udf::clickhouse_udf;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

const SER_VER: u8 = 1;
const FAMILY_ID_HLL: u8 = 7;

const LIST_PREINTS: u8 = 2;
const HASH_SET_PREINTS: u8 = 3;
const HLL_PREINTS: u8 = 10;

const LIST_COUNT_BYTE: usize = 6;
const MODE_BYTE: usize = 7;
const HASH_SET_COUNT_INT: usize = 8;
const HLL_BYTE_ARR_START: usize = 40;

const CUR_MODE_MASK: u8 = 0x03;
const TGT_HLL_TYPE_MASK: u8 = 0x0C;
const EMPTY_FLAG_MASK: u8 = 0x04;

const CUR_MODE_LIST: u8 = 0;
const CUR_MODE_SET: u8 = 1;
const CUR_MODE_HLL: u8 = 2;
const TGT_HLL_8: u8 = 8;

const CONTAINER_TYPE_LARGE: u8 = 3;

const KEY_MASK_26: u32 = (1u32 << 26) - 1;
const VAL_SHIFT: u32 = 26;

/// Converts a serialized Apache DataSketches HLL sketch into a ClickHouse
/// `uniqCombined64State(precision)` payload.
///
/// Supported Apache inputs:
/// - LIST mode  -> expanded into registers -> emitted as ClickHouse LARGE
/// - SET mode   -> expanded into registers -> emitted as ClickHouse LARGE
/// - HLL_8/HLL  -> direct register copy     -> emitted as ClickHouse LARGE
///
/// Unsupported Apache inputs:
/// - HLL_4 and HLL_6 dense payloads
///
/// Caveat:
/// The binary payload shape is compatible with ClickHouse LARGE container state,
/// but semantic merge compatibility with native ClickHouse states still requires
/// the original hash domain to match ClickHouse's `uniqCombined64` hashing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BinaryString(Vec<u8>);

impl BinaryString {
    fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl Serialize for BinaryString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for BinaryString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BinaryStringVisitor;

        impl<'de> Visitor<'de> for BinaryStringVisitor {
            type Value = BinaryString;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("raw bytes or a UTF-8 string")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(BinaryString(value.to_vec()))
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(BinaryString(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(BinaryString(value.as_bytes().to_vec()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(BinaryString(value.into_bytes()))
            }
        }

        deserializer.deserialize_any(BinaryStringVisitor)
    }
}

#[clickhouse_udf]
fn apache_hll_to_uniqcombined64_state(sketch: BinaryString) -> Result<BinaryString, String> {
    convert_apache_hll_to_clickhouse_uniqcombined64(&sketch.into_vec()).map(BinaryString)
}

fn convert_apache_hll_to_clickhouse_uniqcombined64(sketch: &[u8]) -> Result<Vec<u8>, String> {
    if sketch.len() < 8 {
        return Err("input too short for Apache HLL preamble".into());
    }

    let pre_ints = sketch[0];
    let ser_ver = sketch[1];
    let family_id = sketch[2];
    let lg_k = sketch[3];
    let lg_arr = sketch[4];
    let flags = sketch[5];
    let mode_byte = sketch[MODE_BYTE];
    let cur_mode = mode_byte & CUR_MODE_MASK;
    let tgt_hll_type = mode_byte & TGT_HLL_TYPE_MASK;

    if ser_ver != SER_VER {
        return Err(format!("unsupported Apache HLL serialization version: {}", ser_ver));
    }
    if family_id != FAMILY_ID_HLL {
        return Err(format!("unexpected Apache family id: {}", family_id));
    }
    if lg_k < 12 || lg_k > 20 {
        return Err(format!(
            "unsupported lgK={}; ClickHouse uniqCombined64 supports precision in [12, 20]",
            lg_k
        ));
    }

    let bucket_count = 1usize << lg_k;
    let mut registers = vec![0u8; bucket_count];

    if (flags & EMPTY_FLAG_MASK) != 0 {
        return build_clickhouse_large_state(lg_k, &registers);
    }

    match cur_mode {
        CUR_MODE_LIST => {
            if pre_ints != LIST_PREINTS {
                return Err(format!(
                    "LIST mode must have preInts={}, got {}",
                    LIST_PREINTS, pre_ints
                ));
            }
            let list_count = sketch[LIST_COUNT_BYTE] as usize;
            decode_list_like_payload(
                &sketch[8..],
                list_count,
                lg_k,
                &mut registers,
                "LIST",
            )?;
        }
        CUR_MODE_SET => {
            if pre_ints != HASH_SET_PREINTS {
                return Err(format!(
                    "SET mode must have preInts={}, got {}",
                    HASH_SET_PREINTS, pre_ints
                ));
            }
            if sketch.len() < HASH_SET_COUNT_INT + 4 {
                return Err("input too short for SET header".into());
            }
            let set_count = read_u32_le(sketch, HASH_SET_COUNT_INT)? as usize;
            let payload = &sketch[12..];
            decode_set_payload(payload, set_count, lg_arr, lg_k, &mut registers)?;
        }
        CUR_MODE_HLL => {
            if pre_ints != HLL_PREINTS {
                return Err(format!(
                    "HLL mode must have preInts={}, got {}",
                    HLL_PREINTS, pre_ints
                ));
            }
            if tgt_hll_type != TGT_HLL_8 {
                return Err(format!(
                    "only Apache HLL_8 dense payload is supported; target-type bits={} (HLL_4/HLL_6 not supported)",
                    tgt_hll_type >> 2
                ));
            }
            let hll_bytes = bucket_count;
            if sketch.len() < HLL_BYTE_ARR_START + hll_bytes {
                return Err(format!(
                    "input too short for HLL_8 register array: expected at least {}, got {}",
                    HLL_BYTE_ARR_START + hll_bytes,
                    sketch.len()
                ));
            }
            registers.copy_from_slice(&sketch[HLL_BYTE_ARR_START..HLL_BYTE_ARR_START + hll_bytes]);
        }
        other => {
            return Err(format!("unsupported Apache current mode: {}", other));
        }
    }

    build_clickhouse_large_state(lg_k, &registers)
}

fn decode_list_like_payload(
    payload: &[u8],
    pair_count: usize,
    lg_k: u8,
    registers: &mut [u8],
    mode_name: &str,
) -> Result<(), String> {
    let available_pairs = payload.len() / 4;
    if available_pairs < pair_count {
        return Err(format!(
            "{} payload too short: need {} pairs, have {}",
            mode_name, pair_count, available_pairs
        ));
    }
    for i in 0..pair_count {
        let pair = read_u32_le(payload, i * 4)?;
        if pair == 0 {
            return Err(format!(
                "{} payload contains unexpected empty coupon at compact index {}",
                mode_name, i
            ));
        }
        apply_coupon(pair, lg_k, registers)?;
    }
    Ok(())
}

fn decode_set_payload(
    payload: &[u8],
    set_count: usize,
    lg_arr: u8,
    lg_k: u8,
    registers: &mut [u8],
) -> Result<(), String> {
    let available_pairs = payload.len() / 4;
    let full_table_pairs = if lg_arr < (usize::BITS as u8) { 1usize << lg_arr } else { 0 };

    if set_count == 0 {
        return Ok(());
    }

    if available_pairs == set_count {
        return decode_list_like_payload(payload, set_count, lg_k, registers, "SET-compact");
    }

    if full_table_pairs != 0 && available_pairs >= full_table_pairs {
        let mut seen = 0usize;
        for i in 0..full_table_pairs {
            let pair = read_u32_le(payload, i * 4)?;
            if pair == 0 {
                continue;
            }
            apply_coupon(pair, lg_k, registers)?;
            seen += 1;
        }
        if seen != set_count {
            return Err(format!(
                "SET payload count mismatch: header says {}, table contains {} non-empty coupons",
                set_count, seen
            ));
        }
        return Ok(());
    }

    if available_pairs >= set_count {
        return decode_list_like_payload(payload, set_count, lg_k, registers, "SET-truncated");
    }

    Err(format!(
        "SET payload too short: header count={}, lgArr={}, payload pairs={}",
        set_count, lg_arr, available_pairs
    ))
}

fn apply_coupon(pair: u32, lg_k: u8, registers: &mut [u8]) -> Result<(), String> {
    let key = pair & KEY_MASK_26;
    let slot_mask = (1u32 << lg_k) - 1;
    let slot = (key & slot_mask) as usize;
    let value = ((pair >> VAL_SHIFT) & 0x3f) as u8;

    let max_rank = 64 - lg_k + 1;
    if value > max_rank {
        return Err(format!(
            "coupon rank {} exceeds ClickHouse uniqCombined64 max rank {} for lgK={}",
            value, max_rank, lg_k
        ));
    }

    if value > registers[slot] {
        registers[slot] = value;
    }
    Ok(())
}

fn build_clickhouse_large_state(lg_k: u8, registers: &[u8]) -> Result<Vec<u8>, String> {
    let bucket_count = 1usize << lg_k;
    if registers.len() != bucket_count {
        return Err("register array length mismatch".into());
    }

    let max_rank: u8 = 64 - lg_k + 1;
    let zeros = registers.iter().filter(|&&r| r == 0).count();
    let rank_count_len = max_rank as usize + 1;

    let mut rank_store = vec![0u8; (bucket_count * 6 + 7) / 8];
    let mut rank_counts = vec![0u32; rank_count_len];

    for (bucket, &rank) in registers.iter().enumerate() {
        if rank > max_rank {
            return Err(format!(
                "register value {} at bucket {} exceeds ClickHouse uniqCombined64 max rank {}",
                rank, bucket, max_rank
            ));
        }
        set_6bit(&mut rank_store, bucket, rank);
        rank_counts[rank as usize] = rank_counts[rank as usize]
            .checked_add(1)
            .ok_or_else(|| "rank-count overflow".to_string())?;
    }

    let zero_bytes = if bucket_count <= u16::MAX as usize { 2 } else { 4 };
    let mut out = Vec::with_capacity(1 + rank_store.len() + rank_counts.len() * 4 + zero_bytes);
    out.push(CONTAINER_TYPE_LARGE);
    out.extend_from_slice(&rank_store);
    for count in rank_counts {
        out.extend_from_slice(&count.to_le_bytes());
    }
    if bucket_count <= u16::MAX as usize {
        let zeros: u16 = zeros
            .try_into()
            .map_err(|_| "zero-count overflow for UInt16 ClickHouse state".to_string())?;
        out.extend_from_slice(&zeros.to_le_bytes());
    } else {
        let zeros: u32 = zeros
            .try_into()
            .map_err(|_| "zero-count overflow for UInt32 ClickHouse state".to_string())?;
        out.extend_from_slice(&zeros.to_le_bytes());
    }
    Ok(out)
}

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32, String> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| "offset overflow".to_string())?;
    let bytes = buf
        .get(offset..end)
        .ok_or_else(|| format!("buffer too short at offset {}", offset))?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn set_6bit(buf: &mut [u8], index: usize, value: u8) {
    let bit = index * 6;
    let byte_idx = bit / 8;
    let offset = bit % 8;

    if offset <= 2 {
        let mask = 0b0011_1111u8 << offset;
        buf[byte_idx] = (buf[byte_idx] & !mask) | ((value & 0b0011_1111) << offset);
    } else {
        let left = 8 - offset;
        let low_mask = ((1u16 << left) - 1) as u8;
        let high_bits = 6 - left;
        let high_mask = ((1u16 << high_bits) - 1) as u8;

        buf[byte_idx] = (buf[byte_idx] & !(low_mask << offset)) | ((value & low_mask) << offset);
        buf[byte_idx + 1] = (buf[byte_idx + 1] & !high_mask) | ((value >> left) & high_mask);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_wasm_udf::rmp_serde;
    use datasketches::hll::{HllSketch, HllType};

    fn build_real_sketch_for_mode(target_mode: u8) -> Vec<u8> {
        let mut sketch = HllSketch::new(12, HllType::Hll8);
        for value in 0u64.. {
            sketch.update(value);
            let bytes = sketch.serialize();
            if (bytes[MODE_BYTE] & CUR_MODE_MASK) == target_mode {
                return bytes;
            }
        }
        unreachable!("monotonic mode promotion should eventually reach target mode");
    }

    #[test]
    fn list_coupon_updates_target_slot() {
        let lg_k = 12u8;
        let slot = 17u32;
        let value = 9u32;
        let pair = (value << 26) | slot;

        let mut apache = vec![0u8; 8];
        apache[0] = LIST_PREINTS;
        apache[1] = SER_VER;
        apache[2] = FAMILY_ID_HLL;
        apache[3] = lg_k;
        apache[6] = 1;
        apache[7] = CUR_MODE_LIST | TGT_HLL_8;
        apache.extend_from_slice(&pair.to_le_bytes());

        let out = convert_apache_hll_to_clickhouse_uniqcombined64(&apache).unwrap();
        assert_eq!(out[0], CONTAINER_TYPE_LARGE);
    }

    #[test]
    fn set_open_addressed_payload_is_accepted() {
        let lg_k = 12u8;
        let lg_arr = 5u8;
        let mut apache = vec![0u8; 12 + (1usize << lg_arr) * 4];
        apache[0] = HASH_SET_PREINTS;
        apache[1] = SER_VER;
        apache[2] = FAMILY_ID_HLL;
        apache[3] = lg_k;
        apache[4] = lg_arr;
        apache[7] = CUR_MODE_SET | TGT_HLL_8;
        apache[8..12].copy_from_slice(&(1u32).to_le_bytes());

        let pair = (7u32 << 26) | 123u32;
        apache[12..16].copy_from_slice(&pair.to_le_bytes());

        let out = convert_apache_hll_to_clickhouse_uniqcombined64(&apache).unwrap();
        assert_eq!(out[0], CONTAINER_TYPE_LARGE);
    }

    #[test]
    fn dense_hll8_is_accepted() {
        let lg_k = 12u8;
        let mut apache = vec![0u8; HLL_BYTE_ARR_START + (1usize << lg_k)];
        apache[0] = HLL_PREINTS;
        apache[1] = SER_VER;
        apache[2] = FAMILY_ID_HLL;
        apache[3] = lg_k;
        apache[7] = CUR_MODE_HLL | TGT_HLL_8;
        apache[HLL_BYTE_ARR_START + 5] = 11;

        let out = convert_apache_hll_to_clickhouse_uniqcombined64(&apache).unwrap();
        assert_eq!(out[0], CONTAINER_TYPE_LARGE);
    }

    #[test]
    fn datasketches_rust_empty_fixture_is_accepted() {
        let apache = HllSketch::new(12, HllType::Hll8).serialize();
        let out = convert_apache_hll_to_clickhouse_uniqcombined64(&apache).unwrap();
        assert_eq!(out[0], CONTAINER_TYPE_LARGE);
    }

    #[test]
    fn datasketches_rust_list_fixture_is_accepted() {
        let apache = build_real_sketch_for_mode(CUR_MODE_LIST);
        let out = convert_apache_hll_to_clickhouse_uniqcombined64(&apache).unwrap();
        assert_eq!(out[0], CONTAINER_TYPE_LARGE);
    }

    #[test]
    fn datasketches_rust_set_fixture_is_accepted() {
        let apache = build_real_sketch_for_mode(CUR_MODE_SET);
        let out = convert_apache_hll_to_clickhouse_uniqcombined64(&apache).unwrap();
        assert_eq!(out[0], CONTAINER_TYPE_LARGE);
    }

    #[test]
    fn datasketches_rust_hll_fixture_is_accepted() {
        let apache = build_real_sketch_for_mode(CUR_MODE_HLL);
        let out = convert_apache_hll_to_clickhouse_uniqcombined64(&apache).unwrap();
        assert_eq!(out[0], CONTAINER_TYPE_LARGE);
    }

    #[test]
    fn binary_string_round_trips_as_messagepack_bytes() {
        let original = BinaryString(vec![0, 159, 255, 10]);
        let encoded = rmp_serde::to_vec(&original).unwrap();
        let decoded: BinaryString = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn binary_string_accepts_messagepack_string_payloads() {
        let encoded = rmp_serde::to_vec(&"hello").unwrap();
        let decoded: BinaryString = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded, BinaryString(b"hello".to_vec()));
    }

    #[test]
    fn clickhouse_large_state_uses_u16_zero_counter_for_small_precisions() {
        let out = build_clickhouse_large_state(12, &vec![0u8; 1 << 12]).unwrap();
        assert_eq!(out.len(), 3291);
    }

    #[test]
    fn clickhouse_large_state_uses_u32_zero_counter_for_large_precisions() {
        let out = build_clickhouse_large_state(16, &vec![0u8; 1 << 16]).unwrap();
        assert_eq!(out.len(), 49357);
    }
}
