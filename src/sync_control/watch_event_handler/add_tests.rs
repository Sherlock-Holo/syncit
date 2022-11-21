use std::env;
use std::ffi::OsString;
use std::io::Cursor;

use mockall::predicate::*;
use tempfile::TempDir;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use super::*;
use crate::index::{MockIndex, MockIndexGuard};

#[tokio::test]
async fn add_event() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let block_chain = block_chain.clone();

            let mut index_guard = MockIndexGuard::new();
            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(|_| Ok(None));

            index_guard
                .expect_create_file()
                .with(function(move |arg: &IndexFile| {
                    arg.filename == OsStr::new("test.txt")
                        && arg.kind == FileKind::File
                        && arg.detail
                            == FileDetail {
                                gen: 1,
                                hash_sum,
                                block_chain: Some(block_chain.clone()),
                                deleted: false,
                            }
                        && arg.previous_details.is_empty()
                }))
                .returning(|_| Ok(()));

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let (sender, receiver) = flume::bounded::<SendRumors>(1);
    let sender = sender.into_sink();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("test.txt"))
        .await
        .unwrap();

    file.write_all(b"test").await.unwrap();

    let watch_event_handler = WatchEventHandler::new(&user_id, &dir_id, dir.path(), &index, sender);
    watch_event_handler
        .handle_watch_events(vec![WatchEvent::Add {
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
            gen: 1,
            hash_sum,
            block_chain: Some(block_chain),
            deleted: false,
        }
    );
    assert!(rumor.previous_details.is_empty());
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());
}

#[tokio::test]
async fn add_event_with_deleted_index() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let block_chain = block_chain.clone();

            let mut index_guard = MockIndexGuard::new();
            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: "test.txt".into(),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 2,
                            hash_sum: [0; 32],
                            block_chain: None,
                            deleted: true,
                        },
                        previous_details: vec![FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: None,
                            deleted: false,
                        }],
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
                                gen: 3,
                                hash_sum,
                                block_chain: Some(block_chain.clone()),
                                deleted: false,
                            }
                        && arg.previous_details
                            == vec![
                                FileDetail {
                                    gen: 1,
                                    hash_sum,
                                    block_chain: None,
                                    deleted: false,
                                },
                                FileDetail {
                                    gen: 2,
                                    hash_sum: [0; 32],
                                    block_chain: None,
                                    deleted: true,
                                },
                            ]
                }))
                .returning(|_| Ok(()));

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let (sender, receiver) = flume::bounded::<SendRumors>(1);
    let sender = sender.into_sink();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("test.txt"))
        .await
        .unwrap();

    file.write_all(b"test").await.unwrap();

    let watch_event_handler = WatchEventHandler::new(&user_id, &dir_id, dir.path(), &index, sender);
    watch_event_handler
        .handle_watch_events(vec![WatchEvent::Add {
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
            gen: 3,
            hash_sum,
            block_chain: Some(block_chain),
            deleted: false,
        }
    );
    assert_eq!(
        rumor.previous_details,
        vec![
            FileDetail {
                gen: 1,
                hash_sum,
                block_chain: None,
                deleted: false,
            },
            FileDetail {
                gen: 2,
                hash_sum: [0; 32],
                block_chain: None,
                deleted: true,
            },
        ]
    );
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());
}

#[tokio::test]
async fn add_event_with_exists_index() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"123")).await.unwrap();
    let (new_hash_sum, new_block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();
        let new_block_chain = new_block_chain.clone();

        index.expect_begin().returning(move || {
            let block_chain = block_chain.clone();
            let new_block_chain = new_block_chain.clone();

            let mut index_guard = MockIndexGuard::new();
            {
                index_guard
                    .expect_get_file()
                    .with(eq(OsStr::new("test.txt")))
                    .returning(move |_| {
                        Ok(Some(IndexFile {
                            filename: "test.txt".into(),
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
            }

            index_guard
                .expect_update_file()
                .with(function(move |arg: &IndexFile| {
                    arg.filename == OsStr::new("test.txt")
                        && arg.kind == FileKind::File
                        && arg.detail
                            == FileDetail {
                                gen: 2,
                                hash_sum: new_hash_sum,
                                block_chain: Some(new_block_chain.clone()),
                                deleted: false,
                            }
                        && arg.previous_details
                            == vec![FileDetail {
                                gen: 1,
                                hash_sum,
                                block_chain: None,
                                deleted: false,
                            }]
                }))
                .returning(|_| Ok(()));

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let (sender, receiver) = flume::bounded::<SendRumors>(1);
    let sender = sender.into_sink();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("test.txt"))
        .await
        .unwrap();

    file.write_all(b"test").await.unwrap();

    let watch_event_handler = WatchEventHandler::new(&user_id, &dir_id, dir.path(), &index, sender);
    watch_event_handler
        .handle_watch_events(vec![WatchEvent::Add {
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
            hash_sum: new_hash_sum,
            block_chain: Some(new_block_chain),
            deleted: false,
        }
    );
    assert_eq!(
        rumor.previous_details,
        vec![FileDetail {
            gen: 1,
            hash_sum,
            block_chain: None,
            deleted: false,
        },]
    );
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());
}

#[tokio::test]
async fn add_event_with_same_index() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let block_chain = block_chain.clone();

            let mut index_guard = MockIndexGuard::new();
            {
                let block_chain = block_chain.clone();

                index_guard
                    .expect_get_file()
                    .with(eq(OsStr::new("test.txt")))
                    .returning(move |_| {
                        Ok(Some(IndexFile {
                            filename: "test.txt".into(),
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
            }

            index_guard
                .expect_update_file()
                .with(function(move |arg: &IndexFile| {
                    arg.filename == OsStr::new("test.txt")
                        && arg.kind == FileKind::File
                        && arg.detail
                            == FileDetail {
                                gen: 2,
                                hash_sum,
                                block_chain: Some(block_chain.clone()),
                                deleted: false,
                            }
                        && arg.previous_details
                            == vec![FileDetail {
                                gen: 1,
                                hash_sum,
                                block_chain: None,
                                deleted: false,
                            }]
                }))
                .returning(|_| Ok(()));

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let (sender, receiver) = flume::bounded::<SendRumors>(1);
    let sender = sender.into_sink();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("test.txt"))
        .await
        .unwrap();

    file.write_all(b"test").await.unwrap();

    let watch_event_handler = WatchEventHandler::new(&user_id, &dir_id, dir.path(), &index, sender);
    watch_event_handler
        .handle_watch_events(vec![WatchEvent::Add {
            name: OsString::from("test.txt"),
        }])
        .await
        .unwrap();

    receiver.recv_async().await.unwrap_err();
}
