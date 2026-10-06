use slint::{ComponentHandle, ModelRc, VecModel};
use tokio::sync::mpsc;

use crate::core::events::{AppUpdate, AuthStage, UiAction};
use crate::{AppWindow, ChatData, MessageData};

pub struct UiBridge;

impl UiBridge {
    pub fn setup_callbacks(
        window: &AppWindow,
        action_tx: mpsc::Sender<UiAction>,
    ) {
        // 1. Phone submission
        let tx = action_tx.clone();
        let win_weak = window.as_weak();
        window.on_submit_phone(move |phone| {
            if let Some(win) = win_weak.upgrade() {
                win.set_auth_busy(true);
                win.set_auth_error("".into());
            }
            let _ = tx.try_send(UiAction::SubmitPhone(phone.to_string()));
        });

        // 2. Code submission
        let tx = action_tx.clone();
        let win_weak = window.as_weak();
        window.on_submit_code(move |code| {
            if let Some(win) = win_weak.upgrade() {
                win.set_auth_busy(true);
                win.set_auth_error("".into());
            }
            let _ = tx.try_send(UiAction::SubmitCode(code.to_string()));
        });

        // 3. Password submission
        let tx = action_tx.clone();
        let win_weak = window.as_weak();
        window.on_submit_password(move |password| {
            if let Some(win) = win_weak.upgrade() {
                win.set_auth_busy(true);
                win.set_auth_error("".into());
            }
            let _ = tx.try_send(UiAction::SubmitPassword(password.to_string()));
        });

        // 4. Chat selected
        let tx = action_tx.clone();
        window.on_chat_selected(move |chat_id| {
            let _ = tx.try_send(UiAction::SelectChat(chat_id.to_string()));
        });

        // 5. Search chats
        let tx = action_tx.clone();
        window.on_search_chats(move |query| {
            let _ = tx.try_send(UiAction::SearchChats(query.to_string()));
        });

        // 6. Send message
        let tx = action_tx.clone();
        window.on_send_message(move |chat_id, text| {
            let _ = tx.try_send(UiAction::SendMessage {
                chat_id: chat_id.to_string(),
                text: text.to_string(),
            });
        });

        // 6. Log out
        let tx = action_tx;
        window.on_logout_clicked(move || {
            let _ = tx.try_send(UiAction::LogOut);
        });
    }

    pub fn start_update_listener(
        window_weak: slint::Weak<AppWindow>,
        mut update_rx: mpsc::Receiver<AppUpdate>,
    ) {
        tokio::spawn(async move {
            while let Some(update) = update_rx.recv().await {
                let weak = window_weak.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(win) = weak.upgrade() {
                        Self::apply_update(&win, update);
                    }
                });
            }
        });
    }

    fn apply_update(window: &AppWindow, update: AppUpdate) {
        match update {
            AppUpdate::AuthStateChanged { stage, prompt, error } => {
                match stage {
                    AuthStage::Loading => {
                        window.set_auth_stage("loading".into());
                    }
                    AuthStage::WaitPhoneNumber => {
                        window.set_is_authenticated(false);
                        window.set_auth_stage("phone".into());
                    }
                    AuthStage::WaitCode => {
                        window.set_is_authenticated(false);
                        window.set_auth_stage("code".into());
                    }
                    AuthStage::WaitPassword => {
                        window.set_is_authenticated(false);
                        window.set_auth_stage("password".into());
                    }
                    AuthStage::Ready => {
                        window.set_is_authenticated(true);
                    }
                }

                window.set_auth_prompt(prompt.into());
                window.set_auth_error(error.unwrap_or_default().into());
                window.set_auth_busy(false);
            }
            AppUpdate::ConnectionStateChanged { status, state_type } => {
                window.set_connection_status(status.into());
                window.set_connection_state_type(state_type.into());
            }
            AppUpdate::CurrentUserUpdated(name) => {
                window.set_current_user_name(name.into());
            }
            AppUpdate::ChatListUpdated(chats) => {
                let slint_chats: Vec<ChatData> = chats
                    .into_iter()
                    .map(|c| ChatData {
                        chat_id: c.chat_id.into(),
                        title: c.title.into(),
                        last_message: c.last_message.into(),
                        time_text: c.time_text.into(),
                        unread_count: c.unread_count,
                    })
                    .collect();

                let model = ModelRc::new(VecModel::from(slint_chats));
                window.set_chats(model);
            }
            AppUpdate::ActiveChatMessagesUpdated {
                chat_id,
                chat_title,
                messages,
            } => {
                window.set_selected_chat_id(chat_id.into());
                window.set_selected_chat_title(chat_title.into());

                let slint_msgs: Vec<MessageData> = messages
                    .into_iter()
                    .map(|m| MessageData {
                        message_id: m.message_id.into(),
                        sender_name: m.sender_name.into(),
                        text: m.text.into(),
                        time_text: m.time_text.into(),
                        is_outgoing: m.is_outgoing,
                    })
                    .collect();

                let model = ModelRc::new(VecModel::from(slint_msgs));
                window.set_active_messages(model);
            }
            AppUpdate::ShowError(err) => {
                window.set_auth_error(err.into());
            }
        }
    }
}
