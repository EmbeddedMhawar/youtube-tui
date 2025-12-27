use ratatui::layout::Rect;
use tui_additions::widgets::TextField;
use typemap::{CloneMap, Key, TypeMap};
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;

use crate::config::Provider;

#[derive(Clone, Default, Debug)]
pub struct AiProgress {
    pub label: String,
    pub current_chunk: usize,
    pub total_chunks: usize,
    pub eta_seconds: Option<u64>,
    pub ratio: Option<f32>,
    pub active: bool,
}

lazy_static! {
    pub static ref SHARED_AI_PROGRESS: Arc<Mutex<AiProgress>> = Arc::new(Mutex::new(AiProgress::default()));
    pub static ref SELECTED_SUBTITLE: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    pub static ref SELECTED_QUALITY: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    pub static ref SHARED_MENU_INJECTION: Arc<Mutex<Option<Vec<(String, String)>>>> = Arc::new(Mutex::new(None));
}

/// a struct for storing different info, currently only stores one info
#[derive(Clone)]
pub struct Status {
    /// is there is a popup opened
    pub popup_opened: bool,
    /// is search filter popup opened
    pub search_filter_opened: bool,
    /// to prevent rerendering the same image
    pub render_image: bool,
    /// the textfield for command capture
    pub command_capture: Option<TextField>,
    /// used for command history and stuff
    pub command_history_index: Option<usize>,
    /// currently editing command
    pub command_editing_cache: String,
    /// if true, exit in the next iteration
    pub exit: bool,
    /// stores the area of the previously rendered frame
    pub prev_frame: Option<Rect>,
    /// stores global provider (yt/inv)
    pub provider: Provider,
    /// if provider is updated, lasts for 1 event loop
    pub provider_updated: bool,
    /// storage that is cleared every event loop
    pub storage: CloneMap,
}

impl Key for Status {
    type Value = Self;
}

impl Default for Status {
    fn default() -> Self {
        Self {
            popup_opened: false,
            search_filter_opened: false,
            render_image: true,
            command_capture: None,
            exit: false,
            command_history_index: None,
            command_editing_cache: String::new(),
            prev_frame: None,
            provider: Provider::YouTube,
            provider_updated: false,
            storage: TypeMap::custom(),
        }
    }
}

impl Status {
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl Status {
    pub fn reset_command_capture(&mut self) {
        self.command_capture = Some(TextField::default());
        self.command_history_index = None;
        self.command_editing_cache = String::new();
    }
}
