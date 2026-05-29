//! 从远程 ZIP 读取 `version.txt`
//!
//! ```bash
//! cargo run -p neon-helper --example read_version_txt
//! ```

use std::io::Read;

use anyhow::{Context, Result};

const URL: &str = "https://ota.cdn.sunmi.com/OTA/2y5Fp5PPeEOnLFVdOyk9rw6va2p.zip";
const TARGET: &str = "version.txt";

fn main() -> Result<()> {
    let archive = neon_helper::httpzip::RemoteArchive::open(URL).context("open remote zip")?;

    let entry = archive
        .entries
        .iter()
        .find(|e| e.name == TARGET || e.name.ends_with(&format!("/{TARGET}")))
        .with_context(|| {
            let names: Vec<_> = archive.entries.iter().map(|e| e.name.as_str()).collect();
            format!("{TARGET} not found; entries: {names:?}")
        })?;

    let mut reader = entry.open().context("open entry")?;
    let mut content = String::new();
    reader.read_to_string(&mut content).context("read entry body")?;

    print!("{content}");
    if !content.ends_with('\n') {
        println!();
    }
    Ok(())
}
