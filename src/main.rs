slint::include_modules!();

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let main_window = AppWindow::new()?;

    let window_weak = main_window.as_weak();
    main_window.on_connect_clicked(move || {
        if let Some(window) = window_weak.upgrade() {
            window.set_status_text("Connecting to Telegram via TDLib...".into());
        }
    });

    main_window.run()?;
    Ok(())
}
