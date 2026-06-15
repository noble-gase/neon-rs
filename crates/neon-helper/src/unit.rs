use std::fmt;

const KB: i64 = 1024;
const MB: i64 = 1024 * KB;
const GB: i64 = 1024 * MB;
const TB: i64 = 1024 * GB;
const PB: i64 = 1024 * TB;

/// 字节单位（二进制 1024 进制）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteSize {
    B(i64),
    KiB(i64),
    MiB(i64),
    GiB(i64),
    TiB(i64),
    PiB(i64),
}

impl ByteSize {
    /// 转为字节数
    pub const fn to_bytes(self) -> i64 {
        match self {
            ByteSize::B(n) => n,
            ByteSize::KiB(n) => n * KB,
            ByteSize::MiB(n) => n * MB,
            ByteSize::GiB(n) => n * GB,
            ByteSize::TiB(n) => n * TB,
            ByteSize::PiB(n) => n * PB,
        }
    }

    /// 自动选择最合适的单位（用于显示）
    fn best_display(self) -> (f64, &'static str) {
        let bytes = self.to_bytes();

        if bytes >= PB {
            (bytes as f64 / PB as f64, "PB")
        } else if bytes >= TB {
            (bytes as f64 / TB as f64, "TB")
        } else if bytes >= GB {
            (bytes as f64 / GB as f64, "GB")
        } else if bytes >= MB {
            (bytes as f64 / MB as f64, "MB")
        } else if bytes >= KB {
            (bytes as f64 / KB as f64, "KB")
        } else {
            (bytes as f64, "B")
        }
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (value, suffix) = self.best_display();
        if suffix == "B" {
            write!(f, "{value:.0}{suffix}")
        } else {
            write!(f, "{value:.2}{suffix}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ByteSize;

    #[test]
    fn to_bytes_converts_each_unit() {
        assert_eq!(ByteSize::B(42).to_bytes(), 42);
        assert_eq!(ByteSize::KiB(2).to_bytes(), 2 * 1024);
        assert_eq!(ByteSize::MiB(3).to_bytes(), 3 * 1024 * 1024);
        assert_eq!(ByteSize::GiB(4).to_bytes(), 4 * 1024 * 1024 * 1024);
        assert_eq!(ByteSize::TiB(5).to_bytes(), 5 * 1024_i64.pow(4));
        assert_eq!(ByteSize::PiB(6).to_bytes(), 6 * 1024_i64.pow(5));
    }

    #[test]
    fn display_bytes_without_fraction() {
        assert_eq!(ByteSize::B(0).to_string(), "0B");
        assert_eq!(ByteSize::B(512).to_string(), "512B");
        assert_eq!(ByteSize::B(1023).to_string(), "1023B");
    }

    #[test]
    fn display_picks_unit_at_thresholds() {
        assert_eq!(ByteSize::B(1024).to_string(), "1.00KB");
        assert_eq!(ByteSize::KiB(1).to_string(), "1.00KB");
        assert_eq!(ByteSize::MiB(1).to_string(), "1.00MB");
        assert_eq!(ByteSize::GiB(1).to_string(), "1.00GB");
        assert_eq!(ByteSize::TiB(1).to_string(), "1.00TB");
    }

    #[test]
    fn display_fractional_larger_units() {
        assert_eq!(ByteSize::B(1536).to_string(), "1.50KB");
        assert_eq!(ByteSize::KiB(1536).to_string(), "1.50MB");
    }
}
