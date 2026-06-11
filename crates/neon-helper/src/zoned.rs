//! Unix 时间戳 / [`time::OffsetDateTime`] → [`jiff::Zoned`] 转换

use jiff::{Timestamp, Zoned, tz::TimeZone};
use time::OffsetDateTime;

/// strtime 格式：`%Y-%m-%d %H:%M:%S`
pub const DATE_TIME: &str = "%Y-%m-%d %H:%M:%S";

/// strtime 格式：`%Y-%m-%d`
pub const DATE_ONLY: &str = "%Y-%m-%d";

/// strtime 格式：`%H:%M:%S`
pub const TIME_ONLY: &str = "%H:%M:%S";

/// 转为 [`jiff::Zoned`]
pub trait ToZoned {
    /// 系统本地时区
    fn to_system_zoned(&self) -> anyhow::Result<Zoned>;

    /// 指定 IANA 时区（如 `"Asia/Shanghai"`）
    fn to_zoned_in_tz(&self, tz: &str) -> anyhow::Result<Zoned>;
}

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

/// Unix 时间戳（秒 / 毫秒 / 微秒 / 纳秒）
#[derive(Clone, Copy)]
pub enum UnixTime {
    Sec(i64),
    Milli(i64),
    Micro(i64),
    Nano(i128),
}

impl UnixTime {
    fn to_timestamp(self) -> anyhow::Result<Timestamp> {
        match self {
            UnixTime::Sec(v) => Timestamp::from_second(v).map_err(anyhow::Error::from),
            UnixTime::Milli(v) => Timestamp::from_millisecond(v).map_err(anyhow::Error::from),
            UnixTime::Micro(v) => Timestamp::from_microsecond(v).map_err(anyhow::Error::from),
            UnixTime::Nano(v) => Timestamp::from_nanosecond(v).map_err(anyhow::Error::from),
        }
    }
}

impl ToZoned for UnixTime {
    fn to_system_zoned(&self) -> anyhow::Result<Zoned> {
        Ok(self.to_timestamp()?.to_zoned(TimeZone::system()))
    }
    fn to_zoned_in_tz(&self, tz: &str) -> anyhow::Result<Zoned> {
        Ok(self.to_timestamp()?.in_tz(tz)?)
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
        println!(
            "{}",
            strtime::format(zoned::DATE_TIME, &system_zoned).unwrap()
        );

        let zoned_in_tz = odt.to_zoned_in_tz("Asia/Shanghai").unwrap();
        assert_eq!(
            strtime::format(zoned::DATE_TIME, &zoned_in_tz).unwrap(),
            "2019-07-12 13:34:56"
        );
    }

    #[test]
    fn unix_timestamp_to_zoned() {
        // second
        let sec_system_zoned = UnixTime::Sec(1_562_909_696).to_system_zoned().unwrap();
        println!(
            "{}",
            strtime::format(zoned::DATE_TIME, &sec_system_zoned).unwrap()
        );

        let sec_zoned_in_tz = UnixTime::Sec(1_562_909_696)
            .to_zoned_in_tz("Asia/Shanghai")
            .unwrap();
        assert_eq!(
            strtime::format(zoned::DATE_TIME, &sec_zoned_in_tz).unwrap(),
            "2019-07-12 13:34:56"
        );

        // millisecond
        let sec_system_zoned = UnixTime::Milli(1_562_909_696_000)
            .to_system_zoned()
            .unwrap();
        println!(
            "{}",
            strtime::format(zoned::DATE_TIME, &sec_system_zoned).unwrap()
        );

        let sec_zoned_in_tz = UnixTime::Milli(1_562_909_696_000)
            .to_zoned_in_tz("Asia/Shanghai")
            .unwrap();
        assert_eq!(
            strtime::format(zoned::DATE_TIME, &sec_zoned_in_tz).unwrap(),
            "2019-07-12 13:34:56"
        );

        // microsecond
        let sec_system_zoned = UnixTime::Micro(1_562_909_696_000_000)
            .to_system_zoned()
            .unwrap();
        println!(
            "{}",
            strtime::format(zoned::DATE_TIME, &sec_system_zoned).unwrap()
        );

        let sec_zoned_in_tz = UnixTime::Micro(1_562_909_696_000_000)
            .to_zoned_in_tz("Asia/Shanghai")
            .unwrap();
        assert_eq!(
            strtime::format(zoned::DATE_TIME, &sec_zoned_in_tz).unwrap(),
            "2019-07-12 13:34:56"
        );

        // nanosecond
        let sec_system_zoned = UnixTime::Nano(1_562_909_696_000_000_000)
            .to_system_zoned()
            .unwrap();
        println!(
            "{}",
            strtime::format(zoned::DATE_TIME, &sec_system_zoned).unwrap()
        );

        let sec_zoned_in_tz = UnixTime::Nano(1_562_909_696_000_000_000)
            .to_zoned_in_tz("Asia/Shanghai")
            .unwrap();
        assert_eq!(
            strtime::format(zoned::DATE_TIME, &sec_zoned_in_tz).unwrap(),
            "2019-07-12 13:34:56"
        );
    }
}
