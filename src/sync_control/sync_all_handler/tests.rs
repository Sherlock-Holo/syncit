use std::env;
use std::io::Cursor;

use futures_util::stream;
use mockall::predicate::*;
use tempfile::TempDir;

use super::*;
use crate::index::{MockIndex, MockIndexGuard};

#[tokio::test]
async fn all_empty() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    index.expect_begin().returning(move || {
        let mut index_guard = MockIndexGuard::new();
        index_guard
            .expect_list_all_files()
            .times(1)
            .returning(|| Ok(Box::pin(stream::iter([]))));

        index_guard
            .expect_list_all_files()
            .times(1)
            .returning(|| Ok(Box::pin(stream::iter([]))));

        index_guard.expect_commit().returning(|| Ok(()));

        Ok(index_guard)
    });

    let (sender, receiver) = flume::bounded(1);

    let handler = SyncAllHandler::new(&user_id, &dir_id, dir.path(), &index, sender.into_sink());

    handler.handle_sync_all_event().await.unwrap();

    assert_eq!(
        receiver.recv_async().await.unwrap(),
        SendRumors {
            rumors: vec![],
            except: None,
        }
    );
}

#[tokio::test]
async fn empty_index() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let update_time = SystemTime::now();
    let mut index = MockIndex::new();

    fs::write(dir.path().join("test.txt"), b"test")
        .await
        .unwrap();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let block_chain = block_chain.clone();

            let mut index_guard = MockIndexGuard::new();
            index_guard
                .expect_list_all_files()
                .times(1)
                .returning(|| Ok(Box::pin(stream::iter([]))));

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(|_| Ok(None));

            {
                let block_chain = block_chain.clone();

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
                            && arg.update_by == user_id.as_hyphenated().to_string()
                    }))
                    .returning(|_| Ok(()));
            }

            index_guard
                .expect_list_all_files()
                .times(1)
                .returning(move || {
                    Ok(Box::pin(stream::iter([Ok(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: Some(block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time,
                        update_by: user_id.as_hyphenated().to_string(),
                    })])))
                });

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let (sender, receiver) = flume::bounded(1);

    let handler = SyncAllHandler::new(&user_id, &dir_id, dir.path(), &index, sender.into_sink());

    handler.handle_sync_all_event().await.unwrap();

    let mut rumors = receiver.recv_async().await.unwrap();
    assert!(rumors.except.is_none());
    assert_eq!(rumors.rumors.len(), 1);

    let rumor = rumors.rumors.remove(0);

    assert_eq!(rumor.filename, OsStr::new("test.txt"));
    assert_eq!(rumor.kind, FileKind::File);
    assert_eq!(
        rumor.detail,
        FileDetail {
            gen: 1,
            hash_sum,
            block_chain: Some(block_chain.clone()),
            deleted: false,
        }
    );
    assert!(rumor.previous_details.is_empty());
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());
}

#[tokio::test]
async fn empty_dir_with_index() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let update_time = SystemTime::now();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();

            {
                let block_chain = block_chain.clone();
                index_guard
                    .expect_list_all_files()
                    .times(1)
                    .returning(move || {
                        Ok(Box::pin(stream::iter([Ok(IndexFile {
                            filename: OsString::from("test.txt"),
                            kind: FileKind::File,
                            detail: FileDetail {
                                gen: 1,
                                hash_sum,
                                block_chain: Some(block_chain.clone()),
                                deleted: false,
                            },
                            previous_details: vec![],
                            update_time,
                            update_by: user_id.as_hyphenated().to_string(),
                        })])))
                    });
            }

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
                        update_time,
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

            index_guard
                .expect_list_all_files()
                .times(1)
                .returning(move || {
                    Ok(Box::pin(stream::iter([Ok(IndexFile {
                        filename: OsString::from("test.txt"),
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
                        update_time,
                        update_by: user_id.as_hyphenated().to_string(),
                    })])))
                });

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let (sender, receiver) = flume::bounded(1);

    let handler = SyncAllHandler::new(&user_id, &dir_id, dir.path(), &index, sender.into_sink());

    handler.handle_sync_all_event().await.unwrap();

    let mut rumors = receiver.recv_async().await.unwrap();
    assert!(rumors.except.is_none());
    assert_eq!(rumors.rumors.len(), 1);

    let rumor = rumors.rumors.remove(0);

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
async fn index_old() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let update_time = SystemTime::now();
    let mut index = MockIndex::new();

    fs::write(dir.path().join("test.txt"), b"new")
        .await
        .unwrap();

    let (old_hash_sum, old_block_chain) = hash_file(Cursor::new(b"old")).await.unwrap();
    let (new_hash_sum, new_block_chain) = hash_file(Cursor::new(b"new")).await.unwrap();

    {
        let old_block_chain = old_block_chain.clone();
        let new_block_chain = new_block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();

            {
                let old_block_chain = old_block_chain.clone();

                index_guard
                    .expect_list_all_files()
                    .times(1)
                    .returning(move || {
                        Ok(Box::pin(stream::iter([Ok(IndexFile {
                            filename: OsString::from("test.txt"),
                            kind: FileKind::File,
                            detail: FileDetail {
                                gen: 1,
                                hash_sum: old_hash_sum,
                                block_chain: Some(old_block_chain.clone()),
                                deleted: false,
                            },
                            previous_details: vec![],
                            update_time,
                            update_by: user_id.as_hyphenated().to_string(),
                        })])))
                    });
            }

            let old_block_chain = old_block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum: old_hash_sum,
                            block_chain: Some(old_block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time,
                        update_by: user_id.as_hyphenated().to_string(),
                    }))
                });

            {
                let new_block_chain = new_block_chain.clone();

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
                                    hash_sum: old_hash_sum,
                                    block_chain: None,
                                    deleted: false,
                                }]
                            && arg.update_by == user_id.as_hyphenated().to_string()
                    }))
                    .returning(|_| Ok(()));
            }

            let new_block_chain = new_block_chain.clone();

            index_guard
                .expect_list_all_files()
                .times(1)
                .returning(move || {
                    Ok(Box::pin(stream::iter([Ok(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 2,
                            hash_sum: new_hash_sum,
                            block_chain: Some(new_block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![FileDetail {
                            gen: 1,
                            hash_sum: old_hash_sum,
                            block_chain: None,
                            deleted: false,
                        }],
                        update_time,
                        update_by: user_id.as_hyphenated().to_string(),
                    })])))
                });

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let (sender, receiver) = flume::bounded(1);

    let handler = SyncAllHandler::new(&user_id, &dir_id, dir.path(), &index, sender.into_sink());

    handler.handle_sync_all_event().await.unwrap();

    let mut rumors = receiver.recv_async().await.unwrap();
    assert!(rumors.except.is_none());
    assert_eq!(rumors.rumors.len(), 1);

    let rumor = rumors.rumors.remove(0);

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
            hash_sum: old_hash_sum,
            block_chain: None,
            deleted: false,
        }]
    );
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());
}

#[tokio::test]
async fn no_changed() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let update_time = SystemTime::now();
    let mut index = MockIndex::new();

    fs::write(dir.path().join("test.txt"), b"test")
        .await
        .unwrap();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();

            {
                let block_chain = block_chain.clone();

                index_guard
                    .expect_list_all_files()
                    .times(1)
                    .returning(move || {
                        Ok(Box::pin(stream::iter([Ok(IndexFile {
                            filename: OsString::from("test.txt"),
                            kind: FileKind::File,
                            detail: FileDetail {
                                gen: 1,
                                hash_sum,
                                block_chain: Some(block_chain.clone()),
                                deleted: false,
                            },
                            previous_details: vec![],
                            update_time,
                            update_by: user_id.as_hyphenated().to_string(),
                        })])))
                    });
            }

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
                            update_time,
                            update_by: user_id.as_hyphenated().to_string(),
                        }))
                    });
            }

            let block_chain = block_chain.clone();

            index_guard
                .expect_list_all_files()
                .times(1)
                .returning(move || {
                    Ok(Box::pin(stream::iter([Ok(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: Some(block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time,
                        update_by: user_id.as_hyphenated().to_string(),
                    })])))
                });

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let (sender, receiver) = flume::bounded(1);

    let handler = SyncAllHandler::new(&user_id, &dir_id, dir.path(), &index, sender.into_sink());

    handler.handle_sync_all_event().await.unwrap();

    let mut rumors = receiver.recv_async().await.unwrap();
    assert!(rumors.except.is_none());
    assert_eq!(rumors.rumors.len(), 1);

    let rumor = rumors.rumors.remove(0);

    assert_eq!(rumor.filename, OsStr::new("test.txt"));
    assert_eq!(rumor.kind, FileKind::File);
    assert_eq!(
        rumor.detail,
        FileDetail {
            gen: 1,
            hash_sum,
            block_chain: Some(block_chain.clone()),
            deleted: false,
        }
    );
    assert!(rumor.previous_details.is_empty());
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());
}
