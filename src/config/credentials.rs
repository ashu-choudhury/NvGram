//! Telegram API Credentials
//!
//! You can obtain your own Telegram API credentials from https://my.telegram.org:
//! 1. Log in to https://my.telegram.org with your phone number.
//! 2. Go to "API development tools".
//! 3. Create an application (or copy your existing app credentials).
//! 4. Paste your `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` below.
//!
//! Alternatively, you can specify them via the environment variables:
//! `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`.

/// Your Telegram API ID.
/// Replace this integer with your own `api_id` from https://my.telegram.org.
pub const TELEGRAM_API_ID: i32 = 2839;

/// Your Telegram API Hash.
/// Replace this string with your own `api_hash` from https://my.telegram.org.
pub const TELEGRAM_API_HASH: &str = "40a5a36323c4f52e240fa12285e61388";
