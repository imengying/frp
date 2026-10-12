//! 允许的远端端口范围。

use anyhow::{Result, anyhow, bail};

/// 远端端口白名单，例如 `"6000,7000-7100"`；空列表表示不限制。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PortAllowList {
    ranges: Vec<(u16, u16)>,
}

impl PortAllowList {
    /// 解析配置字符串，逗号分隔，支持 `单个端口` 与 `起-止` 两种写法。
    pub fn parse(spec: &str) -> Result<Self> {
        let mut ranges = Vec::new();
        for item in spec.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            let (start, end) = match item.split_once('-') {
                Some((a, b)) => (parse_port(a.trim())?, parse_port(b.trim())?),
                None => {
                    let port = parse_port(item)?;
                    (port, port)
                }
            };
            if start > end {
                bail!("端口范围 `{item}` 的起始端口大于结束端口");
            }
            ranges.push((start, end));
        }
        Ok(Self { ranges })
    }

    /// 是否未配置任何限制。
    pub fn is_unrestricted(&self) -> bool {
        self.ranges.is_empty()
    }

    /// 判断某个端口是否被允许。
    pub fn allows(&self, port: u16) -> bool {
        self.ranges.is_empty() || self.ranges.iter().any(|(s, e)| (*s..=*e).contains(&port))
    }

    /// 用于日志展示。
    pub fn describe(&self) -> String {
        if self.is_unrestricted() {
            "不限制".to_string()
        } else {
            self.ranges
                .iter()
                .map(|(s, e)| {
                    if s == e {
                        s.to_string()
                    } else {
                        format!("{s}-{e}")
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        }
    }
}

fn parse_port(text: &str) -> Result<u16> {
    text.parse::<u16>()
        .map_err(|_| anyhow!("`{text}` 不是合法的端口号"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_means_unrestricted() {
        let list = PortAllowList::parse("").unwrap();
        assert!(list.is_unrestricted());
        assert!(list.allows(1));
        assert!(list.allows(65535));
    }

    #[test]
    fn single_and_range() {
        let list = PortAllowList::parse(" 6000 , 7000-7002 ").unwrap();
        assert!(list.allows(6000));
        assert!(!list.allows(6001));
        assert!(list.allows(7000));
        assert!(list.allows(7002));
        assert!(!list.allows(7003));
        assert_eq!(list.describe(), "6000,7000-7002");
    }

    #[test]
    fn rejects_bad_spec() {
        assert!(PortAllowList::parse("abc").is_err());
        assert!(PortAllowList::parse("7000-6000").is_err());
        assert!(PortAllowList::parse("99999").is_err());
        assert!(PortAllowList::parse("7000-").is_err());
    }
}
