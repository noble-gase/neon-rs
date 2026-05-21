use jiff::{Timestamp, Zoned, tz::TimeZone};
use time::OffsetDateTime;

/// `jiff::fmt::strtime` 格式：年-月-日 时:分:秒
pub const DATE_TIME: &str = "%Y-%m-%d %H:%M:%S";

/// `jiff::fmt::strtime` 格式：年-月-日
pub const DATE_ONLY: &str = "%Y-%m-%d";

/// `jiff::fmt::strtime` 格式：时:分:秒
pub const TIME_ONLY: &str = "%H:%M:%S";

/// Trait: 将不同时间类型统一转换为 jiff::Zoned
pub trait ToZoned {
    /// 转为系统本地时区的 jiff::Zoned
    fn to_system_zoned(&self) -> anyhow::Result<Zoned>;

    /// 转为指定时区的 jiff::Zoned
    fn to_zoned_in_tz(&self, tz: &str) -> anyhow::Result<Zoned>;
}

// ------------------- time::OffsetDateTime -------------------
impl ToZoned for OffsetDateTime {
    fn to_system_zoned(&self) -> anyhow::Result<Zoned> {
        let ts = Timestamp::from_nanosecond(self.unix_timestamp_nanos())?;
        Ok(ts.to_zoned(TimeZone::system()))
    }

    fn to_zoned_in_tz(&self, tz: &str) -> anyhow::Result<Zoned> {
        let ts = Timestamp::from_nanosecond(self.unix_timestamp_nanos())?;
        Ok(ts.in_tz(tz)?)
    }
}

// ------------------- Unix timestamp -------------------
pub enum UnixTime {
    Sec(i64),
    Milli(i64),
    Micro(i64),
    Nano(i128),
}

impl ToZoned for UnixTime {
    fn to_system_zoned(&self) -> anyhow::Result<Zoned> {
        let tz = TimeZone::system();

        match self {
            UnixTime::Sec(v) => Ok(Timestamp::from_second(*v)?.to_zoned(tz)),
            UnixTime::Milli(v) => Ok(Timestamp::from_millisecond(*v)?.to_zoned(tz)),
            UnixTime::Micro(v) => Ok(Timestamp::from_microsecond(*v)?.to_zoned(tz)),
            UnixTime::Nano(v) => Ok(Timestamp::from_nanosecond(*v)?.to_zoned(tz)),
        }
    }

    fn to_zoned_in_tz(&self, tz: &str) -> anyhow::Result<Zoned> {
        match self {
            UnixTime::Sec(v) => Ok(Timestamp::from_second(*v)?.in_tz(tz)?),
            UnixTime::Milli(v) => Ok(Timestamp::from_millisecond(*v)?.in_tz(tz)?),
            UnixTime::Micro(v) => Ok(Timestamp::from_microsecond(*v)?.in_tz(tz)?),
            UnixTime::Nano(v) => Ok(Timestamp::from_nanosecond(*v)?.in_tz(tz)?),
        }
    }
}

#[cfg(test)]
mod tests {
    use jiff::fmt::strtime;
    use time::OffsetDateTime;

    use crate::zoned::ToZoned;
    use crate::zoned::{self, UnixTime};

    #[test]
    fn offset_datetime_to_zoned() {
        let odt = OffsetDateTime::from_unix_timestamp(1562909696).unwrap();

        let system_zoned = odt.to_system_zoned().unwrap();
        println!("{}", strtime::format(zoned::DATE_TIME, &system_zoned).unwrap());

        let zoned_in_tz = odt.to_zoned_in_tz("Asia/Shanghai").unwrap();
        assert_eq!(strtime::format(zoned::DATE_TIME, &zoned_in_tz).unwrap(), "2019-07-12 13:34:56");
    }

    #[test]
    fn unix_timestamp_to_zoned() {
        // second
        let sec_system_zoned = UnixTime::Sec(1_562_909_696).to_system_zoned().unwrap();
        println!("{}", strtime::format(zoned::DATE_TIME, &sec_system_zoned).unwrap());

        let sec_zoned_in_tz = UnixTime::Sec(1_562_909_696).to_zoned_in_tz("Asia/Shanghai").unwrap();
        assert_eq!(strtime::format(zoned::DATE_TIME, &sec_zoned_in_tz).unwrap(), "2019-07-12 13:34:56");

        // millisecond
        let sec_system_zoned = UnixTime::Milli(1_562_909_696_000).to_system_zoned().unwrap();
        println!("{}", strtime::format(zoned::DATE_TIME, &sec_system_zoned).unwrap());

        let sec_zoned_in_tz = UnixTime::Milli(1_562_909_696_000).to_zoned_in_tz("Asia/Shanghai").unwrap();
        assert_eq!(strtime::format(zoned::DATE_TIME, &sec_zoned_in_tz).unwrap(), "2019-07-12 13:34:56");

        // microsecond
        let sec_system_zoned = UnixTime::Micro(1_562_909_696_000_000).to_system_zoned().unwrap();
        println!("{}", strtime::format(zoned::DATE_TIME, &sec_system_zoned).unwrap());

        let sec_zoned_in_tz = UnixTime::Micro(1_562_909_696_000_000).to_zoned_in_tz("Asia/Shanghai").unwrap();
        assert_eq!(strtime::format(zoned::DATE_TIME, &sec_zoned_in_tz).unwrap(), "2019-07-12 13:34:56");

        // nanosecond
        let sec_system_zoned = UnixTime::Nano(1_562_909_696_000_000_000).to_system_zoned().unwrap();
        println!("{}", strtime::format(zoned::DATE_TIME, &sec_system_zoned).unwrap());

        let sec_zoned_in_tz = UnixTime::Nano(1_562_909_696_000_000_000)
            .to_zoned_in_tz("Asia/Shanghai")
            .unwrap();
        assert_eq!(strtime::format(zoned::DATE_TIME, &sec_zoned_in_tz).unwrap(), "2019-07-12 13:34:56");
    }
}
