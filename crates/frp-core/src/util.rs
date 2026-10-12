//! 小工具函数。

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 生成一个 64 位随机数。
///
/// 使用 `RandomState`（进程级随机种子）+ 系统时间纳秒 + 自增计数混合，
/// 再用 xorshift64* 打散，避免为此引入 `rand` 依赖；足够用于
/// run_id / nonce / 重连抖动这类场景。
pub fn random_u64() -> u64 {
    let entropy = RandomState::new().build_hasher().finish();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);

    let mut state = entropy ^ nanos.rotate_left(17) ^ count.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    state.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// 生成 `bytes * 2` 个字符的十六进制随机串。
pub fn random_hex(bytes: usize) -> String {
    let mut out = String::with_capacity(bytes * 2);
    while out.len() < bytes * 2 {
        let value = random_u64();
        for i in 0..8 {
            if out.len() >= bytes * 2 {
                break;
            }
            out.push_str(&format!("{:02x}", ((value >> (i * 8)) & 0xff) as u8));
        }
    }
    out
}

/// 返回 `[0.0, 1.0)` 之间的随机小数。
pub fn random_fraction() -> f64 {
    (random_u64() >> 11) as f64 / (1u64 << 53) as f64
}

/// 常量时间字符串比较，避免 token 校验被时间侧信道利用。
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn random_hex_has_expected_shape() {
        for bytes in [1, 8, 16, 32] {
            let s = random_hex(bytes);
            assert_eq!(s.len(), bytes * 2, "bytes={bytes}");
            assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn random_hex_is_unique_enough() {
        let set: HashSet<String> = (0..1000).map(|_| random_hex(8)).collect();
        assert_eq!(set.len(), 1000);
    }

    #[test]
    fn random_fraction_in_range() {
        for _ in 0..1000 {
            let v = random_fraction();
            assert!((0.0..1.0).contains(&v));
        }
    }

    #[test]
    fn constant_time_eq_works() {
        assert!(constant_time_eq("secret", "secret"));
        assert!(!constant_time_eq("secret", "secreT"));
        assert!(!constant_time_eq("secret", "secret2"));
        assert!(constant_time_eq("", ""));
    }
}
