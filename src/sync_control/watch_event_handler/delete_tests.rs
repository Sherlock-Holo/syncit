use std::env;
use std::ffi::OsString;
use std::io::Cursor;

use mockall::predicate::*;
use tempfile::TempDir;

use super::*;
use crate::index::{MockIndex, MockIndexGuard};

#[tokio::test]
async fn delete_event() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    index.expect_begin().returning(move || {
        let mut index_guard = MockIndexGuard::new();
        index_guard
            .expect_get_file()
            .with(eq(OsStr::new("test.txt")))
            .returning(|_| Ok(None));

        index_guard.expect_commit().returning(|| Ok(()));

        Ok(index_guard)
    });

    let (sender, receiver) = flume::bounded::<SendRumors>(1);
    let sender = sender.into_sink();

    let watch_event_handler = WatchEventHandler::new(&user_id, &dir_id, dir.path(), &index, sender);
    watch_event_handler
        .handle_watch_events(vec![WatchEvent::Delete {
            name: OsString::from("test.txt"),
        }])
        .await
        .unwrap();

    receiver.recv_async().await.unwrap_err();
}

#[tokio::test]
async fn delete_event_with_exist_file() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    index.expect_begin().returning(move || {
        let mut index_guard = MockIndexGuard::new();

        {
            let block_chain = block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: Some(block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time: SystemTime::now(),
                        update_by: user_id.as_hyphenated().to_string(),
                    }))
                });

            index_guard
                .expect_update_file()
                .with(function(move |arg: &IndexFile| {
                    arg.filename == OsStr::new("test.txt")
                        && arg.kind == FileKind::File
                        && arg.detail
                            == FileDetail {
                                gen: 2,
                                hash_sum: [0; 32],
                                block_chain: None,
                                deleted: true,
                            }
                        && arg.previous_details
                            == vec![FileDetail {
                                gen: 1,
                                hash_sum,
                                block_chain: None,
                                deleted: false,
                            }]
                        && arg.update_by == user_id.as_hyphenated().to_string()
                }))
                .returning(|_| Ok(()));
        }

        index_guard.expect_commit().returning(|| Ok(()));

        Ok(index_guard)
    });

    let (sender, receiver) = flume::bounded::<SendRumors>(1);
    let sender = sender.into_sink();

    let watch_event_handler = WatchEventHandler::new(&user_id, &dir_id, dir.path(), &index, sender);
    watch_event_handler
        .handle_watch_events(vec![WatchEvent::Delete {
            name: OsString::from("test.txt"),
        }])
        .await
        .unwrap();

    let mut send_rumors = receiver.recv_async().await.unwrap();

    assert_eq!(send_rumors.dir_id, dir_id);
    assert!(send_rumors.except.is_none());
    assert_eq!(send_rumors.rumors.len(), 1);
    let rumor = send_rumors.rumors.remove(0);

    assert_eq!(rumor.filename, OsStr::new("test.txt"));
    assert_eq!(rumor.kind, FileKind::File);
    assert_eq!(
        rumor.detail,
        FileDetail {
            gen: 2,
            hash_sum: [0; 32],
            block_chain: None,
            deleted: true,
        }
    );
    assert_eq!(
        rumor.previous_details,
        vec![FileDetail {
            gen: 1,
            hash_sum,
            block_chain: None,
            deleted: false,
        }]
    );
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());
}

#[tokio::test]
async fn delete_event_with_deleted_file() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    index.expect_begin().returning(move || {
        let mut index_guard = MockIndexGuard::new();

        {
            let block_chain = block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 2,
                            hash_sum,
                            block_chain: None,
                            deleted: true,
                        },
                        previous_details: vec![FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: Some(block_chain.clone()),
                            deleted: false,
                        }],
                        update_time: SystemTime::now(),
                        update_by: user_id.as_hyphenated().to_string(),
                    }))
                });
        }

        index_guard.expect_commit().returning(|| Ok(()));

        Ok(index_guard)
    });

    let (sender, receiver) = flume::bounded::<SendRumors>(1);
    let sender = sender.into_sink();

    let watch_event_handler = WatchEventHandler::new(&user_id, &dir_id, dir.path(), &index, sender);
    watch_event_handler
        .handle_watch_events(vec![WatchEvent::Delete {
            name: OsString::from("test.txt"),
        }])
        .await
        .unwrap();

    receiver.recv_async().await.unwrap_err();
}
