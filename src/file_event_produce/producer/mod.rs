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

            let mut rename_events = HashMap::new();
            let mut all_watch_events = Vec::with_capacity(events.len());

            for event in events {
                let watch_events = match &event.kind {
                    EventKind::Any | EventKind::Other => event
                        .paths
                        .into_iter()
                        .map(|path| WatchEvent::Modify {
                            name: path.into_os_string(),
                        })
                        .collect::<Vec<_>>(),
                    EventKind::Access(_) => continue,
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
                            continue;
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
                                match rename_mode {
                                    RenameMode::Any | RenameMode::Other => {
                                        warn!(?rename_mode, paths = ?event.paths, "unhandled rename event, ignore");

                                        continue;
                                    }
                                    RenameMode::To | RenameMode::From => {
                                        match event.paths.get(0) {
                                            None => {
                                                warn!(?event, "no paths event, ignore");

                                                continue;
                                            }
                                            Some(path) => {
                                                rename_events.insert(path.clone(), event);
                                            }
                                        }

                                        continue;
                                    }
                                    RenameMode::Both => {
                                        if event.paths.len() != 2 {
                                            warn!(
                                                ?event,
                                                "rename event doesn't have 2 path, ignore"
                                            );

                                            continue;
                                        }

                                        let from = &event.paths[0];
                                        let to = &event.paths[1];

                                        // in dir rename, remove from and to event, replace with
                                        // both event
                                        rename_events.remove(from);
                                        rename_events.remove(to);

                                        rename_events.insert(from.clone(), event);

                                        continue;
                                    }
                                }
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
                            continue;
                        }
                    }
                };

                all_watch_events.extend(watch_events);
            }

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

            self.sync_control_event_sender
                .send(Event::Watch(all_watch_events))
                .await
                .map_err(Into::into)
                .tap_err(|err| error!(%err, "send watch events to sync control failed"))?;
        }

        warn!(dir = ?self.dir, "dir watcher is stopped");

        Err(io::Error::new(
            IoErrorKind::Other,
            format!("{:?}, dir watcher is stopped", self.dir),
        ))
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
    if let ErrorKind::Io(err) = err.kind {
        return err;
    }

    io::Error::new(IoErrorKind::Other, err)
}
