use std::ffi::OsString;

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
