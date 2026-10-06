mod config;
mod core;
mod telegram;
mod ui_bridge;

use tokio::sync::mpsc;

use config::AppConfig;
use core::events::{AppUpdate, UiAction};
use telegram::TelegramService;
use ui_bridge::UiBridge;

slint::include_modules!();

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Initialize configuration and directories
    let config = match AppConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Failed to load configuration: {e}");
            return Err(e);
        }
    };

    // 2. Initialize asynchronous event channels
    // action_tx: UI -> Telegram Service
    let (action_tx, action_rx) = mpsc::channel::<UiAction>(100);
    // update_tx: Telegram Service -> UI
    let (update_tx, update_rx) = mpsc::channel::<AppUpdate>(100);

    // 3. Initialize the Slint application window
    let window = AppWindow::new()?;

    // 4. Hook Slint UI callbacks to dispatch UiActions into Tokio channel
    UiBridge::setup_callbacks(&window, action_tx);

    // 5. Start background listener forwarding AppUpdates into Slint event loop
    UiBridge::start_update_listener(window.as_weak(), update_rx);

    // 6. Start the Telegram Service background task loop
    let service = TelegramService::new(config, update_tx, action_rx);
    service.start();

    // 7. Run the Slint UI main event loop
    window.run()?;

    Ok(())
}
