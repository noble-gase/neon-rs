use std::panic::Location;

/// 为错误消息附加调用位置（`文件:行号`）。
#[track_caller]
pub fn make(msg: impl AsRef<str>) -> String {
    let loc = Location::caller();
    format!("{} ({}:{})", msg.as_ref(), loc.file(), loc.line())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make() {
        let s = make("test");
        println!("location = {s}");
    }
}
