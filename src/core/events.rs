#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthStage {
    Loading,
    WaitPhoneNumber,
    WaitCode,
    WaitPassword,
    Ready,
}

#[derive(Debug, Clone)]
pub struct ChatViewModel {
    pub chat_id: String,
    pub title: String,
    pub last_message: String,
    pub time_text: String,
    pub unread_count: i32,
}

#[derive(Debug, Clone)]
pub struct MessageViewModel {
    pub message_id: String,
    pub sender_name: String,
    pub text: String,
    pub time_text: String,
    pub is_outgoing: bool,
}

#[derive(Debug, Clone)]
pub enum UiAction {
    SubmitPhone(String),
    SubmitCode(String),
    SubmitPassword(String),
    SelectChat(String),
    SearchChats(String),
    SendMessage { chat_id: String, text: String },
    LogOut,
}

#[derive(Debug, Clone)]
pub enum AppUpdate {
    AuthStateChanged {
        stage: AuthStage,
        prompt: String,
        error: Option<String>,
    },
    ConnectionStateChanged {
        status: String,
        state_type: String, // "online", "connecting", "updating", "offline"
    },
    CurrentUserUpdated(String),
    ChatListUpdated(Vec<ChatViewModel>),
    ActiveChatMessagesUpdated {
        chat_id: String,
        chat_title: String,
        messages: Vec<MessageViewModel>,
    },
    ShowError(String),
}
