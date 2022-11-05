use uuid::Uuid;

use crate::file_event_produce::WatchEvent;
use crate::index::IndexFile;

#[derive(Debug)]
pub enum Event {
    Watch(Vec<WatchEvent>),

    Rumors {
        sender_id: Uuid,
        remote_index: Vec<IndexFile>,
    },

    SyncAll,
}
