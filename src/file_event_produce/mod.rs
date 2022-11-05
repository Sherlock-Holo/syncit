use std::error::Error;
use std::ffi::OsString;

mod producer;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum WatchEvent {
    Add {
        name: OsString,
    },

    Modify {
        name: OsString,
    },

    Rename {
        old_name: OsString,
        new_name: OsString,
    },

    Delete {
        name: OsString,
    },
}

pub trait WatchControl {
    type Error: Error;

    async fn pause_watch(&mut self) -> Result<(), Self::Error>;

    async fn resume_watch(&mut self) -> Result<(), Self::Error>;
}
