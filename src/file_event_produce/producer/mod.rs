use std::collections::HashMap;
use std::io::{self, ErrorKind as IoErrorKind};
use std::path::PathBuf;
use std::task::{Context, Poll};

use flume::Receiver;
use futures_util::task::noop_waker_ref;
use futures_util::{Sink, SinkExt, TryStreamExt};
use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{
    ErrorKind, Event as NotifyEvent, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use tap::TapFallible;
use tracing::{error, warn};

use crate::file_event_produce::{WatchControl, WatchEvent};
use crate::sync_control::event::Event;

pub struct Producer<Si> {
    dir: PathBuf,
    receiver: Receiver<Result<NotifyEvent, notify::Error>>,
    sync_control_event_sender: Si,
}

impl<Si> Producer<Si> {
    pub fn new(dir: PathBuf, sync_control_event_sender: Si) -> io::Result<(Self, Controller)> {
        let (sender, receiver) = flume::unbounded();

        let dir_watcher =
            notify::recommended_watcher(move |event: Result<NotifyEvent, notify::Error>| {
                if let Err(err) = sender.send(event) {
                    error!(%err, "send watch event failed");
                }
            })
            .map_err(notify_err_to_io_err)?;

        Ok((
            Self {
                dir: dir.clone(),
                receiver,
                sync_control_event_sender,
            },
            Controller { dir, dir_watcher },
        ))
    }
}

impl<Si> Producer<Si>
where
    Si: Sink<Event> + Unpin,
    Si::Error: Into<io::Error>,
{
    pub async fn run(&mut self) -> io::Result<()> {
        let mut receiver_stream = self.receiver.stream();

        while let Some(event) = receiver_stream.try_next().await.map_err(|err| {
            error!(%err, "receive event from watcher failed");

            notify_err_to_io_err(err)
        })? {
            let mut events = vec![event];
            // try to collect more events but without await
            loop {
                match receiver_stream
                    .try_poll_next_unpin(&mut Context::from_waker(noop_waker_ref()))
                    .map_err(|err| {
                        error!(%err, "receive event from watcher failed");

                        notify_err_to_io_err(err)
                    })? {
                    Poll::Ready(None) => {
                        error!(dir = ?self.dir, "watcher is stopped unexpectedly");

                        return Err(io::Error::new(
                            IoErrorKind::Other,
                            format!("dir: {:?}, watcher is stopped unexpectedly", self.dir),
                        ));
                    }
                    Poll::Ready(Some(event)) => events.push(event),
                    Poll::Pending => {
                        break;
                    }
                }
            }

            Self::handle_events(&mut self.sync_control_event_sender, events).await?;
        }

        warn!(dir = ?self.dir, "dir watcher is stopped");

        Err(io::Error::new(
            IoErrorKind::Other,
            format!("{:?}, dir watcher is stopped", self.dir),
        ))
    }

    async fn handle_events(
        sync_control_event_sender: &mut Si,
        events: Vec<NotifyEvent>,
    ) -> io::Result<()> {
        let mut rename_events = HashMap::new();
        let mut all_watch_events = Vec::with_capacity(events.len());

        for event in events {
            let watch_events = Self::create_watch_events(&mut rename_events, event);
            if let Some(watch_events) = watch_events {
                all_watch_events.extend(watch_events);
            }
        }

        Self::compose_rename_events(rename_events, &mut all_watch_events);

        sync_control_event_sender
            .send(Event::Watch(all_watch_events))
            .await
            .map_err(Into::into)
            .tap_err(|err| error!(%err, "send watch events to sync control failed"))?;

        Ok(())
    }

    fn create_watch_events(
        rename_events: &mut HashMap<PathBuf, NotifyEvent>,
        event: NotifyEvent,
    ) -> Option<Vec<WatchEvent>> {
        let watch_events = match &event.kind {
            EventKind::Any | EventKind::Other => event
                .paths
                .into_iter()
                .map(|path| WatchEvent::Modify {
                    name: path.into_os_string(),
                })
                .collect::<Vec<_>>(),
            EventKind::Access(_) => return None,
            EventKind::Create(create_kind) => {
                if matches!(
                    create_kind,
                    CreateKind::Other | CreateKind::File | CreateKind::Any
                ) {
                    event
                        .paths
                        .into_iter()
                        .map(|path| WatchEvent::Add {
                            name: path.into_os_string(),
                        })
                        .collect::<Vec<_>>()
                } else {
                    return None;
                }
            }
            EventKind::Modify(modify_kind) => {
                match modify_kind {
                    ModifyKind::Any
                    | ModifyKind::Data(_)
                    | ModifyKind::Metadata(_)
                    | ModifyKind::Other => event
                        .paths
                        .into_iter()
                        .map(|path| WatchEvent::Modify {
                            name: path.into_os_string(),
                        })
                        .collect::<Vec<_>>(),
                    ModifyKind::Name(rename_mode) => {
                        return match rename_mode {
                            RenameMode::Any | RenameMode::Other => {
                                warn!(?rename_mode, paths = ?event.paths, "unhandled rename event, ignore");

                                None
                            }
                            RenameMode::To | RenameMode::From => {
                                match event.paths.get(0) {
                                    None => {
                                        warn!(?event, "no paths event, ignore");

                                        return None;
                                    }
                                    Some(path) => {
                                        rename_events.insert(path.clone(), event);
                                    }
                                }

                                None
                            }
                            RenameMode::Both => {
                                if event.paths.len() != 2 {
                                    warn!(?event, "rename event doesn't have 2 path, ignore");

                                    return None;
                                }

                                let from = &event.paths[0];
                                let to = &event.paths[1];

                                // in dir rename, remove from and to event, replace with both event
                                rename_events.remove(from);
                                rename_events.remove(to);

                                rename_events.insert(from.clone(), event);

                                None
                            }
                        };
                    }
                }
            }
            EventKind::Remove(remove_kind) => {
                if matches!(
                    remove_kind,
                    RemoveKind::Any | RemoveKind::File | RemoveKind::Other
                ) {
                    event
                        .paths
                        .into_iter()
                        .map(|path| WatchEvent::Delete {
                            name: path.into_os_string(),
                        })
                        .collect::<Vec<_>>()
                } else {
                    return None;
                }
            }
        };

        Some(watch_events)
    }

    fn compose_rename_events(
        rename_events: HashMap<PathBuf, NotifyEvent>,
        all_watch_events: &mut Vec<WatchEvent>,
    ) {
        for mut event in rename_events.into_values() {
            match event.kind {
                EventKind::Modify(ModifyKind::Name(rename_mode)) => match rename_mode {
                    RenameMode::Any | RenameMode::Other => unreachable!(),
                    RenameMode::To => all_watch_events.push(WatchEvent::Add {
                        name: event.paths.remove(0).into_os_string(),
                    }),
                    RenameMode::From => all_watch_events.push(WatchEvent::Delete {
                        name: event.paths.remove(0).into_os_string(),
                    }),
                    RenameMode::Both => {
                        let from = event.paths.remove(0);
                        let to = event.paths.remove(0);

                        all_watch_events.push(WatchEvent::Rename {
                            old_name: from.into_os_string(),
                            new_name: to.into_os_string(),
                        })
                    }
                },

                _ => unreachable!(),
            }
        }
    }
}

pub struct Controller {
    dir: PathBuf,
    dir_watcher: RecommendedWatcher,
}

impl WatchControl for Controller {
    type Error = io::Error;

    async fn pause_watch(&mut self) -> Result<(), Self::Error> {
        self.dir_watcher
            .unwatch(&self.dir)
            .map_err(notify_err_to_io_err)
    }

    async fn resume_watch(&mut self) -> Result<(), Self::Error> {
        self.dir_watcher
            .watch(&self.dir, RecursiveMode::NonRecursive)
            .map_err(notify_err_to_io_err)
    }
}

fn notify_err_to_io_err(err: notify::Error) -> io::Error {
    match err.kind {
        ErrorKind::Io(err) => err,
        ErrorKind::PathNotFound => io::Error::from(IoErrorKind::NotFound),
        _ => io::Error::new(IoErrorKind::Other, err),
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use tokio::fs;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_create() {
        let temp_dir = tempfile::tempdir_in(env::temp_dir()).unwrap();
        let temp_dir_path = temp_dir.path();
        let (sender, receiver) = flume::unbounded();
        let sender = sender
            .into_sink()
            .sink_map_err(|err| io::Error::new(IoErrorKind::Other, err));
        let (mut producer, mut controller) =
            Producer::new(temp_dir_path.to_path_buf(), sender).unwrap();

        controller.resume_watch().await.unwrap();
        tokio::spawn(async move { producer.run().await });

        let file_path = temp_dir_path.join("test.txt");
        let _file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        let event = receiver.recv_async().await.unwrap();

        controller.pause_watch().await.unwrap();

        let watch_events = match event {
            Event::Watch(watch_events) => watch_events,
            _ => {
                panic!("wrong event type")
            }
        };

        assert_eq!(watch_events.len(), 1);
        assert_eq!(
            &watch_events[0],
            &WatchEvent::Add {
                name: file_path.into_os_string()
            }
        );
    }

    #[tokio::test]
    async fn test_modify() {
        let temp_dir = tempfile::tempdir_in(env::temp_dir()).unwrap();
        let temp_dir_path = temp_dir.path();
        let (sender, receiver) = flume::unbounded();
        let sender = sender
            .into_sink()
            .sink_map_err(|err| io::Error::new(IoErrorKind::Other, err));
        let (mut producer, mut controller) =
            Producer::new(temp_dir_path.to_path_buf(), sender).unwrap();

        let file_path = temp_dir_path.join("test.txt");
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        file.write_all(b"abc").await.unwrap();

        controller.resume_watch().await.unwrap();
        tokio::spawn(async move { producer.run().await });

        file.write_all(b"test").await.unwrap();

        let event = receiver.recv_async().await.unwrap();

        controller.pause_watch().await.unwrap();

        let watch_events = match event {
            Event::Watch(watch_events) => watch_events,
            _ => {
                panic!("wrong event type")
            }
        };

        assert_eq!(watch_events.len(), 1);
        assert_eq!(
            &watch_events[0],
            &WatchEvent::Modify {
                name: file_path.into_os_string()
            }
        );
    }

    #[tokio::test]
    async fn test_rename() {
        let temp_dir = tempfile::tempdir_in(env::temp_dir()).unwrap();
        let temp_dir_path = temp_dir.path();
        let (sender, receiver) = flume::unbounded();
        let sender = sender
            .into_sink()
            .sink_map_err(|err| io::Error::new(IoErrorKind::Other, err));
        let (mut producer, mut controller) =
            Producer::new(temp_dir_path.to_path_buf(), sender).unwrap();

        let file_path = temp_dir_path.join("old.txt");
        let new_file_path = temp_dir_path.join("new.txt");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        controller.resume_watch().await.unwrap();
        tokio::spawn(async move { producer.run().await });

        fs::rename(&file_path, &new_file_path).await.unwrap();

        let event = receiver.recv_async().await.unwrap();

        controller.pause_watch().await.unwrap();

        let watch_events = match event {
            Event::Watch(watch_events) => watch_events,
            _ => {
                panic!("wrong event type")
            }
        };

        assert_eq!(watch_events.len(), 1);
        assert_eq!(
            &watch_events[0],
            &WatchEvent::Rename {
                old_name: file_path.into_os_string(),
                new_name: new_file_path.into_os_string(),
            }
        );
    }

    #[tokio::test]
    async fn test_delete() {
        let temp_dir = tempfile::tempdir_in(env::temp_dir()).unwrap();
        let temp_dir_path = temp_dir.path();
        let (sender, receiver) = flume::unbounded();
        let sender = sender
            .into_sink()
            .sink_map_err(|err| io::Error::new(IoErrorKind::Other, err));
        let (mut producer, mut controller) =
            Producer::new(temp_dir_path.to_path_buf(), sender).unwrap();

        let file_path = temp_dir_path.join("test.txt");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        controller.resume_watch().await.unwrap();
        tokio::spawn(async move { producer.run().await });

        fs::remove_file(&file_path).await.unwrap();

        let event = receiver.recv_async().await.unwrap();

        controller.pause_watch().await.unwrap();

        let watch_events = match event {
            Event::Watch(watch_events) => watch_events,
            _ => {
                panic!("wrong event type")
            }
        };

        assert_eq!(watch_events.len(), 1);
        assert_eq!(
            &watch_events[0],
            &WatchEvent::Delete {
                name: file_path.into_os_string()
            }
        );
    }

    #[tokio::test]
    async fn test_move_in() {
        let temp_dir = tempfile::tempdir_in(env::temp_dir()).unwrap();
        let temp_dir_path = temp_dir.path();
        let sub_dir_path = temp_dir_path.join("sub");
        let (sender, receiver) = flume::unbounded();
        let sender = sender
            .into_sink()
            .sink_map_err(|err| io::Error::new(IoErrorKind::Other, err));
        let (mut producer, mut controller) =
            Producer::new(temp_dir_path.to_path_buf(), sender).unwrap();

        fs::create_dir(&sub_dir_path).await.unwrap();
        let file_path = sub_dir_path.join("test.txt");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        controller.resume_watch().await.unwrap();
        tokio::spawn(async move { producer.run().await });

        let new_file_path = temp_dir_path.join("test.txt");
        fs::rename(file_path, &new_file_path).await.unwrap();

        let event = receiver.recv_async().await.unwrap();

        controller.pause_watch().await.unwrap();

        let watch_events = match event {
            Event::Watch(watch_events) => watch_events,
            _ => {
                panic!("wrong event type")
            }
        };

        assert_eq!(watch_events.len(), 1);
        assert_eq!(
            &watch_events[0],
            &WatchEvent::Add {
                name: new_file_path.into_os_string()
            }
        );
    }

    #[tokio::test]
    async fn test_move_out() {
        let temp_dir = tempfile::tempdir_in(env::temp_dir()).unwrap();
        let temp_dir_path = temp_dir.path();
        let sub_dir_path = temp_dir_path.join("sub");
        let (sender, receiver) = flume::unbounded();
        let sender = sender
            .into_sink()
            .sink_map_err(|err| io::Error::new(IoErrorKind::Other, err));
        let (mut producer, mut controller) =
            Producer::new(temp_dir_path.to_path_buf(), sender).unwrap();

        fs::create_dir(&sub_dir_path).await.unwrap();
        let file_path = temp_dir_path.join("test.txt");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        controller.resume_watch().await.unwrap();
        tokio::spawn(async move { producer.run().await });

        let new_file_path = sub_dir_path.join("test.txt");
        fs::rename(&file_path, &new_file_path).await.unwrap();

        let event = receiver.recv_async().await.unwrap();

        controller.pause_watch().await.unwrap();

        let watch_events = match event {
            Event::Watch(watch_events) => watch_events,
            _ => {
                panic!("wrong event type")
            }
        };

        assert_eq!(watch_events.len(), 1);
        assert_eq!(
            &watch_events[0],
            &WatchEvent::Delete {
                name: file_path.into_os_string()
            }
        );
    }
}
