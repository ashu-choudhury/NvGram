use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use tdlib_rs::enums::{
    AuthorizationState, ConnectionState, InputMessageContent, MessageContent, Update,
};
use tdlib_rs::functions;
use tdlib_rs::types::{FormattedText, InputMessageText};

use crate::config::AppConfig;
use crate::core::events::{
    AppUpdate, AuthStage, ChatViewModel, MessageViewModel, UiAction,
};

pub struct TelegramService {
    config: AppConfig,
    client_id: i32,
    ui_tx: mpsc::Sender<AppUpdate>,
    action_rx: Arc<Mutex<mpsc::Receiver<UiAction>>>,
    chats: Arc<Mutex<HashMap<i64, tdlib_rs::types::Chat>>>,
}

impl TelegramService {
    pub fn new(
        config: AppConfig,
        ui_tx: mpsc::Sender<AppUpdate>,
        action_rx: mpsc::Receiver<UiAction>,
    ) -> Self {
        let client_id = tdlib_rs::create_client();
        Self {
            config,
            client_id,
            ui_tx,
            action_rx: Arc::new(Mutex::new(action_rx)),
            chats: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn start(self) {
        let client_id = self.client_id;
        let config = self.config.clone();
        let ui_tx = self.ui_tx.clone();
        let chats = self.chats.clone();
        let action_rx = self.action_rx.clone();

        // 1. TDLib Receiver Task (reads updates from tdlib)
        let (update_tx, mut update_rx) = mpsc::channel::<Update>(100);
        std::thread::spawn(move || {
            loop {
                if let Some((update, rec_client_id)) = tdlib_rs::receive() {
                    if rec_client_id == client_id {
                        let _ = update_tx.blocking_send(update);
                    }
                }
            }
        });

        // 2. Main Telegram Event Loop
        tokio::spawn(async move {
            let mut active_chat_id: Option<i64> = None;

            loop {
                tokio::select! {
                    Some(update) = update_rx.recv() => {
                        Self::handle_update(
                            client_id,
                            &config,
                            &ui_tx,
                            &chats,
                            update,
                            active_chat_id,
                        ).await;
                    }

                    action = async {
                        let mut rx = action_rx.lock().await;
                        rx.recv().await
                    } => {
                        if let Some(act) = action {
                            Self::handle_action(
                                client_id,
                                &ui_tx,
                                &chats,
                                &mut active_chat_id,
                                act,
                            ).await;
                        } else {
                            break;
                        }
                    }
                }
            }
        });
    }

    async fn handle_update(
        client_id: i32,
        config: &AppConfig,
        ui_tx: &mpsc::Sender<AppUpdate>,
        chats: &Arc<Mutex<HashMap<i64, tdlib_rs::types::Chat>>>,
        update: Update,
        active_chat_id: Option<i64>,
    ) {
        match update {
            Update::AuthorizationState(auth) => {
                Self::handle_auth_state(client_id, config, ui_tx, auth.authorization_state).await;
            }
            Update::ConnectionState(conn) => {
                let (status, state_type) = match conn.state {
                    ConnectionState::Connecting => ("Connecting to Telegram...", "connecting"),
                    ConnectionState::Updating => ("Updating chats & messages...", "updating"),
                    ConnectionState::Ready => ("Connected", "online"),
                    ConnectionState::WaitingForNetwork => ("Waiting for network connection...", "offline"),
                    _ => ("Connecting...", "connecting"),
                };
                let _ = ui_tx
                    .send(AppUpdate::ConnectionStateChanged {
                        status: status.to_string(),
                        state_type: state_type.to_string(),
                    })
                    .await;
            }
            Update::NewChat(new_chat) => {
                let mut map = chats.lock().await;
                map.insert(new_chat.chat.id, new_chat.chat);
                drop(map);
                Self::publish_chat_list(chats, ui_tx).await;
            }
            Update::ChatTitle(title_upd) => {
                let mut map = chats.lock().await;
                if let Some(chat) = map.get_mut(&title_upd.chat_id) {
                    chat.title = title_upd.title;
                }
                drop(map);
                Self::publish_chat_list(chats, ui_tx).await;
            }
            Update::ChatLastMessage(last_msg) => {
                let mut map = chats.lock().await;
                if let Some(chat) = map.get_mut(&last_msg.chat_id) {
                    chat.last_message = last_msg.last_message;
                }
                drop(map);
                Self::publish_chat_list(chats, ui_tx).await;
            }
            Update::NewMessage(new_msg) => {
                // If this belongs to active chat, refresh message view
                if Some(new_msg.message.chat_id) == active_chat_id {
                    Self::fetch_and_publish_chat_history(client_id, new_msg.message.chat_id, chats, ui_tx).await;
                }
            }
            Update::User(u) => {
                let user_name = format!("{} {}", u.user.first_name, u.user.last_name).trim().to_string();
                let _ = ui_tx.send(AppUpdate::CurrentUserUpdated(user_name)).await;
            }
            _ => {}
        }
    }

    async fn handle_auth_state(
        client_id: i32,
        config: &AppConfig,
        ui_tx: &mpsc::Sender<AppUpdate>,
        auth_state: AuthorizationState,
    ) {
        match auth_state {
            AuthorizationState::WaitTdlibParameters => {
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::Loading,
                        prompt: "Configuring TDLib parameters...".into(),
                        error: None,
                    })
                    .await;

                let db_path = config.database_directory.to_string_lossy().to_string();
                let files_path = config.files_directory.to_string_lossy().to_string();

                let res = functions::set_tdlib_parameters(
                    config.use_test_dc,
                    db_path,
                    files_path,
                    "".to_string(), // database_encryption_key
                    true,           // use_file_database
                    true,           // use_chat_info_database
                    true,           // use_message_database
                    true,           // use_secret_chats
                    config.api_id,
                    config.api_hash.clone(),
                    config.system_language_code.clone(),
                    config.device_model.clone(),
                    config.system_version.clone(),
                    config.application_version.clone(),
                    client_id,
                )
                .await;

                if let Err(err) = res {
                    let _ = ui_tx
                        .send(AppUpdate::ShowError(format!(
                            "TDLib initialization failed: {}",
                            err.message
                        )))
                        .await;
                }
            }
            AuthorizationState::WaitPhoneNumber => {
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::WaitPhoneNumber,
                        prompt: "Please enter your phone number with country code (e.g. +1234567890):".into(),
                        error: None,
                    })
                    .await;
            }
            AuthorizationState::WaitCode(_) => {
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::WaitCode,
                        prompt: "Enter the verification code sent to your Telegram account or phone:".into(),
                        error: None,
                    })
                    .await;
            }
            AuthorizationState::WaitPassword(_) => {
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::WaitPassword,
                        prompt: "Your account is protected by 2-Step Verification. Enter your cloud password:".into(),
                        error: None,
                    })
                    .await;
            }
            AuthorizationState::Ready => {
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::Ready,
                        prompt: "Authorized!".into(),
                        error: None,
                    })
                    .await;

                // Load initial chats
                let _ = functions::load_chats(None, 50, client_id).await;

                // Fetch current user details
                if let Ok(tdlib_rs::enums::User::User(user)) = functions::get_me(client_id).await {
                    let user_name = format!("{} {}", user.first_name, user.last_name).trim().to_string();
                    let _ = ui_tx.send(AppUpdate::CurrentUserUpdated(user_name)).await;
                }
            }
            AuthorizationState::Closed => {
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::WaitPhoneNumber,
                        prompt: "Session closed. Please log in again.".into(),
                        error: None,
                    })
                    .await;
            }
            _ => {}
        }
    }

    async fn handle_action(
        client_id: i32,
        ui_tx: &mpsc::Sender<AppUpdate>,
        chats: &Arc<Mutex<HashMap<i64, tdlib_rs::types::Chat>>>,
        active_chat_id: &mut Option<i64>,
        action: UiAction,
    ) {
        match action {
            UiAction::SubmitPhone(phone) => {
                if let Err(err) = functions::set_authentication_phone_number(phone, None, client_id).await {
                    let _ = ui_tx
                        .send(AppUpdate::AuthStateChanged {
                            stage: AuthStage::WaitPhoneNumber,
                            prompt: "Failed to submit phone number.".into(),
                            error: Some(err.message),
                        })
                        .await;
                }
            }
            UiAction::SubmitCode(code) => {
                if let Err(err) = functions::check_authentication_code(code, client_id).await {
                    let _ = ui_tx
                        .send(AppUpdate::AuthStateChanged {
                            stage: AuthStage::WaitCode,
                            prompt: "Verification code incorrect.".into(),
                            error: Some(err.message),
                        })
                        .await;
                }
            }
            UiAction::SubmitPassword(pass) => {
                if let Err(err) = functions::check_authentication_password(pass, client_id).await {
                    let _ = ui_tx
                        .send(AppUpdate::AuthStateChanged {
                            stage: AuthStage::WaitPassword,
                            prompt: "Invalid cloud password.".into(),
                            error: Some(err.message),
                        })
                        .await;
                }
            }
            UiAction::SelectChat(chat_id_str) => {
                if let Ok(id) = chat_id_str.parse::<i64>() {
                    *active_chat_id = Some(id);
                    Self::fetch_and_publish_chat_history(client_id, id, chats, ui_tx).await;
                }
            }
            UiAction::SendMessage { chat_id, text } => {
                if let Ok(id) = chat_id.parse::<i64>() {
                    let content = InputMessageContent::InputMessageText(InputMessageText {
                        text: FormattedText {
                            text,
                            entities: Vec::new(),
                        },
                        link_preview_options: None,
                        clear_draft: true,
                    });
                    if let Err(err) = functions::send_message(id, None, None, None, content, client_id).await {
                        let _ = ui_tx.send(AppUpdate::ShowError(err.message)).await;
                    } else {
                        // Refresh active history
                        Self::fetch_and_publish_chat_history(client_id, id, chats, ui_tx).await;
                    }
                }
            }
            UiAction::LogOut => {
                let _ = functions::log_out(client_id).await;
            }
        }
    }

    async fn publish_chat_list(
        chats: &Arc<Mutex<HashMap<i64, tdlib_rs::types::Chat>>>,
        ui_tx: &mpsc::Sender<AppUpdate>,
    ) {
        let map = chats.lock().await;
        let mut list: Vec<ChatViewModel> = map
            .values()
            .map(|c| {
                let snippet = c
                    .last_message
                    .as_ref()
                    .map(|m| Self::format_content(&m.content))
                    .unwrap_or_default();

                ChatViewModel {
                    chat_id: c.id.to_string(),
                    title: if c.title.is_empty() { "Saved Messages".to_string() } else { c.title.clone() },
                    last_message: snippet,
                    time_text: "".to_string(),
                    unread_count: c.unread_count,
                }
            })
            .collect();

        // Sort alphabetically or by title
        list.sort_by(|a, b| a.title.cmp(&b.title));

        let _ = ui_tx.send(AppUpdate::ChatListUpdated(list)).await;
    }

    async fn fetch_and_publish_chat_history(
        client_id: i32,
        chat_id: i64,
        chats: &Arc<Mutex<HashMap<i64, tdlib_rs::types::Chat>>>,
        ui_tx: &mpsc::Sender<AppUpdate>,
    ) {
        let chat_title = {
            let map = chats.lock().await;
            map.get(&chat_id).map(|c| c.title.clone()).unwrap_or_else(|| "Chat".to_string())
        };

        if let Ok(tdlib_rs::enums::Messages::Messages(msgs)) =
            functions::get_chat_history(chat_id, 0, 0, 50, false, client_id).await
        {
            let mut view_messages: Vec<MessageViewModel> = msgs
                .messages
                .into_iter()
                .filter_map(|m| m)
                .map(|m| MessageViewModel {
                    message_id: m.id.to_string(),
                    sender_name: if m.is_outgoing { "You".to_string() } else { "".to_string() },
                    text: Self::format_content(&m.content),
                    time_text: "".to_string(),
                    is_outgoing: m.is_outgoing,
                })
                .collect();

            // Reverse so oldest is top, newest is bottom
            view_messages.reverse();

            let _ = ui_tx
                .send(AppUpdate::ActiveChatMessagesUpdated {
                    chat_id: chat_id.to_string(),
                    chat_title,
                    messages: view_messages,
                })
                .await;
        }
    }

    fn format_content(content: &MessageContent) -> String {
        match content {
            MessageContent::MessageText(t) => t.text.text.clone(),
            MessageContent::MessagePhoto(p) => {
                if !p.caption.text.is_empty() {
                    format!("📷 {}", p.caption.text)
                } else {
                    "📷 Photo".to_string()
                }
            }
            MessageContent::MessageVideo(v) => {
                if !v.caption.text.is_empty() {
                    format!("📹 {}", v.caption.text)
                } else {
                    "📹 Video".to_string()
                }
            }
            MessageContent::MessageVoiceNote(_) => "🎤 Voice Note".to_string(),
            MessageContent::MessageAudio(a) => format!("🎵 {}", a.audio.file_name),
            MessageContent::MessageDocument(d) => format!("📄 {}", d.document.file_name),
            MessageContent::MessageSticker(s) => format!("🎭 Sticker {}", s.sticker.emoji),
            MessageContent::MessageCall(_) => "📞 Call".to_string(),
            _ => "Telegram message".to_string(),
        }
    }
}
