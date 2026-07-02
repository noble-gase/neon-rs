//! 地理坐标与大地平面直角坐标转换、距离与方位角计算

use std::fmt;
use std::sync::OnceLock;

const WGS84_A: f64 = 6378137.0;
const WGS84_E2: f64 = 0.00669437999013;
const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;
const RAD_TO_DEG: f64 = 180.0 / std::f64::consts::PI;
const FALSE_EASTING: f64 = 500_000.0;
const UTM_SCALE: f64 = 0.9996;
const INV_A0_EPS: f64 = 1e-8;

/// 大地平面直角坐标系点
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
    /// 中央子午线（度），用于投影反算
    pub meridian: i32,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y, meridian: 0 }
    }

    pub fn with_meridian(x: f64, y: f64, meridian: i32) -> Self {
        Self { x, y, meridian }
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(x: {}, y: {})", self.x, self.y)
    }
}

/// 地理坐标（经纬度，单位：度）
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Location {
    pub lng: f64,
    pub lat: f64,
}

impl Location {
    pub fn new(lng: f64, lat: f64) -> Self {
        Self { lng, lat }
    }

    /// 球面距离（米）
    pub fn distance(self, other: Location) -> f64 {
        let lng1 = self.lng * DEG_TO_RAD;
        let lat1 = self.lat * DEG_TO_RAD;
        let lng2 = other.lng * DEG_TO_RAD;
        let lat2 = other.lat * DEG_TO_RAD;
        let theta = lng2 - lng1;
        let cos_dist = lat1.sin() * lat2.sin() + lat1.cos() * lat2.cos() * theta.cos();
        cos_dist.clamp(-1.0, 1.0).acos() * WGS84_A
    }

    /// 方位角（0～360 度）
    pub fn azimuth(self, other: Location) -> f64 {
        if other.lng == self.lng && other.lat == self.lat {
            return 0.0;
        }
        if other.lng == self.lng {
            return if other.lat > self.lat { 0.0 } else { 180.0 };
        }
        if other.lat == self.lat {
            return if other.lng > self.lng { 90.0 } else { 270.0 };
        }

        let a = (90.0 - other.lat) * DEG_TO_RAD;
        let b = (90.0 - self.lat) * DEG_TO_RAD;
        let delta_lng = (other.lng - self.lng) * DEG_TO_RAD;

        let cosc = a.cos() * b.cos() + a.sin() * b.sin() * delta_lng.cos();
        let sinc = (1.0 - cosc * cosc).sqrt();

        let sin_a = (a.sin() * delta_lng.sin() / sinc).clamp(-1.0, 1.0);
        let angle = sin_a.asin() * RAD_TO_DEG;

        if other.lat < self.lat {
            180.0 - angle
        } else if other.lng < self.lng {
            360.0 + angle
        } else {
            angle
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(lng: {}, lat: {})", self.lng, self.lat)
    }
}

/// 投影类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Projection {
    GaussKruger,
    Utm,
}

#[derive(Debug, Clone, Copy)]
struct Ellipsoid {
    a: f64,
    e2: f64,
    ep2: f64,
    a0: f64,
    a2: f64,
    a4: f64,
    a6: f64,
}

static WGS84: OnceLock<Ellipsoid> = OnceLock::new();

impl Ellipsoid {
    fn wgs84() -> Self {
        *WGS84.get_or_init(Self::compute_wgs84)
    }

    fn compute_wgs84() -> Self {
        let a = WGS84_A;
        let e2 = WGS84_E2;
        let b = (a * a * (1.0 - e2)).sqrt();
        let ep2 = (a * a - b * b) / (b * b);

        let m0 = a * (1.0 - e2);
        let m2 = 1.5 * e2 * m0;
        let m4 = 1.25 * e2 * m2;
        let m6 = 7.0 * e2 * m4 / 6.0;
        let m8 = 9.0 * e2 * m6 / 8.0;

        Self {
            a,
            e2,
            ep2,
            a0: m0 + m2 / 2.0 + 3.0 * m4 / 8.0 + 5.0 * m6 / 16.0 + 35.0 * m8 / 128.0,
            a2: m2 / 2.0 + m4 / 2.0 + 15.0 * m6 / 32.0 + 7.0 * m8 / 16.0,
            a4: m4 / 8.0 + 3.0 * m6 / 16.0 + 7.0 * m8 / 32.0,
            a6: m6 / 32.0 + m8 / 16.0,
        }
    }
}

/// WGS84 经纬度与大地平面直角坐标间的投影转换
#[derive(Debug, Clone, Copy)]
pub struct GeoTransform {
    ellipsoid: Ellipsoid,
    meridian: i32,
    projection: Projection,
}

impl GeoTransform {
    pub fn new(meridian: i32, projection: Projection) -> Self {
        Self {
            ellipsoid: Ellipsoid::wgs84(),
            meridian,
            projection,
        }
    }

    /// 经纬度 → 大地平面直角坐标
    pub fn bl2xy(&self, loc: Location) -> Point {
        let ep = self.ellipsoid;

        let mut meridian = self.meridian;
        if meridian < -180 {
            meridian = ((loc.lng + 1.5) / 3.0).trunc() as i32 * 3;
        }

        let lat = loc.lat * DEG_TO_RAD;
        let dl = (loc.lng - f64::from(meridian)) * DEG_TO_RAD;

        let x_meridian = ep.a0 * lat - ep.a2 * (2.0 * lat).sin() / 2.0
            + ep.a4 * (4.0 * lat).sin() / 4.0
            - ep.a6 * (6.0 * lat).sin() / 6.0;

        let sin_lat = lat.sin();
        let cos_lat = lat.cos();
        let cos2 = cos_lat * cos_lat;
        let cos3 = cos2 * cos_lat;
        let cos5 = cos3 * cos2;

        let tn = lat.tan();
        let tn2 = tn * tn;
        let tn4 = tn2 * tn2;
        let j2 = ep.ep2 * cos2;
        let n = ep.a / (1.0 - ep.e2 * sin_lat * sin_lat).sqrt();

        let dl2 = dl * dl;
        let dl3 = dl2 * dl;
        let dl4 = dl2 * dl2;
        let dl5 = dl4 * dl;
        let dl6 = dl4 * dl2;

        let temp0 = n * sin_lat * cos_lat * dl2 / 2.0;
        let temp1 = n * sin_lat * cos3 * (5.0 - tn2 + 9.0 * j2 + 4.0 * j2 * j2) * dl4 / 24.0;
        let temp2 = n * sin_lat * cos5 * (61.0 - 58.0 * tn2 + tn4) * dl6 / 720.0;
        let temp3 = n * cos_lat * dl;
        let temp4 = n * cos3 * (1.0 - tn2 + j2) * dl3 / 6.0;
        let temp5 = n * cos5 * (5.0 - 18.0 * tn2 + tn4 + 14.0 * j2 - 58.0 * tn2 * j2) * dl5 / 120.0;

        let mut x = temp3 + temp4 + temp5;
        let mut y = x_meridian + temp0 + temp1 + temp2;

        match self.projection {
            Projection::GaussKruger => x += FALSE_EASTING,
            Projection::Utm => {
                x = x * UTM_SCALE + FALSE_EASTING;
                y *= UTM_SCALE;
            }
        }

        Point::with_meridian(x, y, meridian)
    }

    /// 大地平面直角坐标 → 经纬度
    pub fn xy2bl(&self, point: Point) -> Location {
        let ep = self.ellipsoid;

        let mut x = point.x - FALSE_EASTING;
        let mut y = point.y;

        if self.projection == Projection::Utm {
            x /= UTM_SCALE;
            y /= UTM_SCALE;
        }

        let mut bf0 = y / ep.a0;
        // 正常输入数次内收敛；设上限防 NaN/无穷大输入下永不收敛导致死循环
        for _ in 0..100 {
            let y0 = -ep.a2 * (2.0 * bf0).sin() / 2.0 + ep.a4 * (4.0 * bf0).sin() / 4.0
                - ep.a6 * (6.0 * bf0).sin() / 6.0;
            let bf = (y - y0) / ep.a0;
            let converged = (bf - bf0).abs() <= INV_A0_EPS;
            bf0 = bf;
            if converged {
                break;
            }
        }

        let sin_bf = bf0.sin();
        let cos_bf = bf0.cos();
        let cos2 = cos_bf * cos_bf;

        let t = sin_bf / cos_bf;
        let t2 = t * t;
        let t4 = t2 * t2;
        let j2 = ep.ep2 * cos2;

        let v = (1.0 - ep.e2 * sin_bf * sin_bf).sqrt();
        let n = ep.a / v;
        let n3 = n * n * n;
        let n5 = n3 * n * n;
        let m = ep.a * (1.0 - ep.e2) / (v * v * v);

        let x2 = x * x;
        let x3 = x2 * x;
        let x4 = x2 * x2;
        let x5 = x4 * x;
        let x6 = x4 * x2;

        let temp0 = t * x2 / (2.0 * m * n);
        let temp1 = t * (5.0 + 3.0 * t2 + j2 - 9.0 * j2 * t2) * x4 / (24.0 * m * n3);
        let temp2 = t * (61.0 + 90.0 * t2 + 45.0 * t4) * x6 / (720.0 * n5 * m);
        let lat = (bf0 - temp0 + temp1 - temp2) * RAD_TO_DEG;

        let temp0 = x / (n * cos_bf);
        let temp1 = (1.0 + 2.0 * t2 + j2) * x3 / (6.0 * n3 * cos_bf);
        let temp2 =
            (5.0 + 28.0 * t2 + 6.0 * j2 + 24.0 * t4 + 8.0 * t2 * j2) * x5 / (120.0 * n5 * cos_bf);
        let lng = (temp0 - temp1 + temp2) * RAD_TO_DEG + f64::from(point.meridian);

        Location::new(lng, lat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance() {
        let loc1 = Location::new(118.63173312, 31.94530239);
        let loc2 = Location::new(118.63343344, 31.94382162);
        assert_eq!(loc1.distance(loc2).round(), 230.0);
    }

    #[test]
    fn azimuth() {
        let loc1 = Location::new(118.63173312, 31.94530239);
        let loc2 = Location::new(118.63343344, 31.94382162);
        let angle = (loc1.azimuth(loc2) * 10.0).round() / 10.0;
        assert_eq!(angle, 135.7);
    }

    #[test]
    fn project_unproject_roundtrip() {
        let transform = GeoTransform::new(-360, Projection::GaussKruger);
        let loc = Location::new(116.300105669, 39.731939769);

        let point = transform.bl2xy(loc);
        assert_eq!(point.x.round(), 440_000.0);
        assert_eq!(point.y.round(), 4_400_000.0);

        let back = transform.xy2bl(point);
        assert!((back.lng - 116.300105669).abs() < 1e-9);
        assert!((back.lat - 39.731939769).abs() < 1e-9);
    }
}
