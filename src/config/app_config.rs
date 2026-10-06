use directories::ProjectDirs;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub api_id: i32,
    pub api_hash: String,
    pub database_directory: PathBuf,
    pub files_directory: PathBuf,
    pub system_language_code: String,
    pub device_model: String,
    pub system_version: String,
    pub application_version: String,
    pub use_test_dc: bool,
}

impl AppConfig {
    pub fn load() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let proj_dirs = ProjectDirs::from("org", "NvGram", "NvGram")
            .ok_or("Failed to determine application data directories")?;

        let data_dir = proj_dirs.data_dir();
        let db_dir = data_dir.join("tdlib_db");
        let files_dir = data_dir.join("tdlib_files");

        fs::create_dir_all(&db_dir)?;
        fs::create_dir_all(&files_dir)?;

use super::credentials::{TELEGRAM_API_HASH, TELEGRAM_API_ID};

        // Load credentials from credentials.rs (or environment overrides if set)
        let api_id = std::env::var("TELEGRAM_API_ID")
            .ok()
            .and_then(|id| id.parse::<i32>().ok())
            .unwrap_or(TELEGRAM_API_ID);

        let api_hash = std::env::var("TELEGRAM_API_HASH")
            .unwrap_or_else(|_| TELEGRAM_API_HASH.to_string());

        let use_test_dc = std::env::var("TELEGRAM_USE_TEST_DC")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        Ok(Self {
            api_id,
            api_hash,
            database_directory: db_dir,
            files_directory: files_dir,
            system_language_code: "en".to_string(),
            device_model: "PC (Windows)".to_string(),
            system_version: "Windows 11".to_string(),
            application_version: env!("CARGO_PKG_VERSION").to_string(),
            use_test_dc,
        })
    }
}
