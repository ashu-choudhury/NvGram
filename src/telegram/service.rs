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
    search_query: Arc<Mutex<String>>,
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
            search_query: Arc::new(Mutex::new(String::new())),
        }
    }

    pub fn start(self) {
        let client_id = self.client_id;
        let config = self.config.clone();
        let ui_tx = self.ui_tx.clone();
        let chats = self.chats.clone();
        let action_rx = self.action_rx.clone();
        let search_query = self.search_query.clone();

        // 1. TDLib Receiver Task (reads updates from tdlib)
        let (update_tx, mut update_rx) = mpsc::unbounded_channel::<Update>();
        std::thread::spawn(move || {
            loop {
                if let Some((update, rec_client_id)) = tdlib_rs::receive() {
                    if rec_client_id == client_id {
                        let _ = update_tx.send(update);
                    }
                }
            }
        });

        // 2. Main Telegram Event Loop
        tokio::spawn(async move {
            let mut active_chat_id: Option<i64> = None;

            println!("[NvGram] Initializing TDLib client {}...", client_id);

            // Set TDLib log verbosity to 1 (Fatal & Critical errors only) to silence verbose C++ logs
            let _ = functions::set_log_verbosity_level(1, client_id).await;

            // Ping TDLib with an initial query so it begins dispatching updates to this client
            let _ = functions::get_option("version".to_string(), client_id).await;
            println!("[NvGram] TDLib client active. Entering event loop.");

            loop {
                tokio::select! {
                    Some(update) = update_rx.recv() => {
                        Self::handle_update(
                            client_id,
                            &config,
                            &ui_tx,
                            &chats,
                            &search_query,
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
                                &search_query,
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
        search_query: &Arc<Mutex<String>>,
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
                Self::publish_chat_list(chats, ui_tx, search_query).await;
            }
            Update::ChatTitle(title_upd) => {
                let mut map = chats.lock().await;
                if let Some(chat) = map.get_mut(&title_upd.chat_id) {
                    chat.title = title_upd.title;
                }
                drop(map);
                Self::publish_chat_list(chats, ui_tx, search_query).await;
            }
            Update::ChatLastMessage(last_msg) => {
                let mut map = chats.lock().await;
                if let Some(chat) = map.get_mut(&last_msg.chat_id) {
                    chat.last_message = last_msg.last_message;
                    chat.positions = last_msg.positions;
                }
                drop(map);
                Self::publish_chat_list(chats, ui_tx, search_query).await;
            }
            Update::ChatPosition(pos_upd) => {
                let mut map = chats.lock().await;
                if let Some(chat) = map.get_mut(&pos_upd.chat_id) {
                    chat.positions.retain(|p| p.list != pos_upd.position.list);
                    if pos_upd.position.order > 0 {
                        chat.positions.push(pos_upd.position);
                    }
                }
                drop(map);
                Self::publish_chat_list(chats, ui_tx, search_query).await;
            }
            Update::ChatReadInbox(read) => {
                let mut map = chats.lock().await;
                if let Some(chat) = map.get_mut(&read.chat_id) {
                    chat.unread_count = read.unread_count;
                }
                drop(map);
                Self::publish_chat_list(chats, ui_tx, search_query).await;
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
                println!("[NvGram] AuthorizationState::WaitTdlibParameters: configuring TDLib...");
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::Loading,
                        prompt: "Configuring TDLib parameters...".into(),
                        error: None,
                    })
                    .await;

                let db_path = config.database_directory.to_string_lossy().to_string();
                let files_path = config.files_directory.to_string_lossy().to_string();

                println!("[NvGram] Setting TDLib parameters: api_id={}, db={}", config.api_id, db_path);

                let cfg = config.clone();
                let ui = ui_tx.clone();
                tokio::spawn(async move {
                    let res = functions::set_tdlib_parameters(
                        cfg.use_test_dc,
                        db_path,
                        files_path,
                        "".to_string(), // database_encryption_key
                        true,           // use_file_database
                        true,           // use_chat_info_database
                        true,           // use_message_database
                        true,           // use_secret_chats
                        cfg.api_id,
                        cfg.api_hash.clone(),
                        cfg.system_language_code.clone(),
                        cfg.device_model.clone(),
                        cfg.system_version.clone(),
                        cfg.application_version.clone(),
                        client_id,
                    )
                    .await;

                    if let Err(err) = res {
                        eprintln!("[NvGram] TDLib initialization error: {}", err.message);
                        let _ = ui
                            .send(AppUpdate::ShowError(format!(
                                "TDLib initialization failed: {}",
                                err.message
                            )))
                            .await;
                    } else {
                        println!("[NvGram] TDLib parameters accepted successfully.");
                    }
                });
            }
            AuthorizationState::WaitPhoneNumber => {
                println!("[NvGram] AuthorizationState::WaitPhoneNumber: Waiting for phone number...");
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::WaitPhoneNumber,
                        prompt: "Please enter your phone number with country code (e.g. +1234567890):".into(),
                        error: None,
                    })
                    .await;
            }
            AuthorizationState::WaitCode(_) => {
                println!("[NvGram] AuthorizationState::WaitCode: Enter verification code.");
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::WaitCode,
                        prompt: "Enter the verification code sent to your Telegram account or phone:".into(),
                        error: None,
                    })
                    .await;
            }
            AuthorizationState::WaitPassword(_) => {
                println!("[NvGram] AuthorizationState::WaitPassword: Enter cloud password.");
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::WaitPassword,
                        prompt: "Your account is protected by 2-Step Verification. Enter your cloud password:".into(),
                        error: None,
                    })
                    .await;
            }
            AuthorizationState::Ready => {
                println!("[NvGram] AuthorizationState::Ready: Authorized!");
                let _ = ui_tx
                    .send(AppUpdate::AuthStateChanged {
                        stage: AuthStage::Ready,
                        prompt: "Authorized!".into(),
                        error: None,
                    })
                    .await;

                // Load initial chats (up to 100)
                let _ = functions::load_chats(None, 100, client_id).await;

                // Fetch current user details
                if let Ok(tdlib_rs::enums::User::User(user)) = functions::get_me(client_id).await {
                    let user_name = format!("{} {}", user.first_name, user.last_name).trim().to_string();
                    let _ = ui_tx.send(AppUpdate::CurrentUserUpdated(user_name)).await;
                }
            }
            AuthorizationState::Closed => {
                println!("[NvGram] AuthorizationState::Closed: Session closed.");
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
        search_query: &Arc<Mutex<String>>,
        active_chat_id: &mut Option<i64>,
        action: UiAction,
    ) {
        match action {
            UiAction::SubmitPhone(phone) => {
                println!("[NvGram] Submitting phone number: {}", phone);
                if let Err(err) = functions::set_authentication_phone_number(phone, None, client_id).await {
                    eprintln!("[NvGram] Phone number error: {}", err.message);
                    let _ = ui_tx
                        .send(AppUpdate::AuthStateChanged {
                            stage: AuthStage::WaitPhoneNumber,
                            prompt: "Failed to submit phone number.".into(),
                            error: Some(err.message),
                        })
                        .await;
                } else {
                    println!("[NvGram] Phone number submitted successfully. Awaiting code...");
                }
            }
            UiAction::SubmitCode(code) => {
                println!("[NvGram] Submitting verification code: {}", code);
                if let Err(err) = functions::check_authentication_code(code, client_id).await {
                    eprintln!("[NvGram] Verification code error: {}", err.message);
                    let _ = ui_tx
                        .send(AppUpdate::AuthStateChanged {
                            stage: AuthStage::WaitCode,
                            prompt: "Verification code incorrect.".into(),
                            error: Some(err.message),
                        })
                        .await;
                } else {
                    println!("[NvGram] Verification code accepted!");
                }
            }
            UiAction::SubmitPassword(pass) => {
                println!("[NvGram] Submitting cloud password...");
                if let Err(err) = functions::check_authentication_password(pass, client_id).await {
                    eprintln!("[NvGram] Cloud password error: {}", err.message);
                    let _ = ui_tx
                        .send(AppUpdate::AuthStateChanged {
                            stage: AuthStage::WaitPassword,
                            prompt: "Invalid cloud password.".into(),
                            error: Some(err.message),
                        })
                        .await;
                } else {
                    println!("[NvGram] Cloud password accepted!");
                }
            }
            UiAction::SelectChat(chat_id_str) => {
                if let Ok(id) = chat_id_str.parse::<i64>() {
                    *active_chat_id = Some(id);
                    Self::fetch_and_publish_chat_history(client_id, id, chats, ui_tx).await;
                }
            }
            UiAction::SearchChats(query) => {
                {
                    let mut q = search_query.lock().await;
                    *q = query.clone();
                }
                Self::publish_chat_list(chats, ui_tx, search_query).await;

                let trimmed = query.trim().to_string();
                if !trimmed.is_empty() {
                    if let Ok(tdlib_rs::enums::Chats::Chats(found)) =
                        functions::search_chats(trimmed, 20, client_id).await
                    {
                        for chat_id in found.chat_ids {
                            if let Ok(tdlib_rs::enums::Chat::Chat(c)) =
                                functions::get_chat(chat_id, client_id).await
                            {
                                let mut map = chats.lock().await;
                                map.insert(c.id, c);
                            }
                        }
                        Self::publish_chat_list(chats, ui_tx, search_query).await;
                    }
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
        search_query: &Arc<Mutex<String>>,
    ) {
        let current_query = {
            let q = search_query.lock().await;
            q.to_lowercase().trim().to_string()
        };

        let map = chats.lock().await;
        let mut list: Vec<(i64, ChatViewModel)> = map
            .values()
            .map(|c| {
                let snippet = c
                    .last_message
                    .as_ref()
                    .map(|m| Self::format_content(&m.content))
                    .unwrap_or_default();

                let (time_text, date) = match &c.last_message {
                    Some(m) => (Self::format_timestamp(m.date), m.date as i64),
                    None => (String::new(), 0),
                };

                // Pinned order or latest message timestamp
                let order = c
                    .positions
                    .iter()
                    .map(|p| p.order)
                    .max()
                    .unwrap_or(0);
                let sort_key = if order > 0 { order } else { date };

                (
                    sort_key,
                    ChatViewModel {
                        chat_id: c.id.to_string(),
                        title: if c.title.is_empty() { "Saved Messages".to_string() } else { c.title.clone() },
                        last_message: snippet,
                        time_text,
                        unread_count: c.unread_count,
                    },
                )
            })
            .collect();

        // Sort descending: most active / recent chats and pinned chats at the top!
        list.sort_by(|a, b| b.0.cmp(&a.0));

        // Filter by search query if user typed into the search bar
        let view_models: Vec<ChatViewModel> = list
            .into_iter()
            .map(|(_, vm)| vm)
            .filter(|c| {
                if current_query.is_empty() {
                    true
                } else {
                    c.title.to_lowercase().contains(&current_query)
                        || c.last_message.to_lowercase().contains(&current_query)
                }
            })
            .collect();

        let _ = ui_tx.send(AppUpdate::ChatListUpdated(view_models)).await;
    }

    fn format_timestamp(ts: i32) -> String {
        if ts <= 0 {
            return String::new();
        }
        let total_minutes = ts / 60;
        let minute = total_minutes % 60;
        let hour = (total_minutes / 60) % 24;
        format!("{:02}:{:02}", hour, minute)
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
