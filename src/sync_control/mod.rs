use std::error::Error;
use std::io;
use std::path::PathBuf;

use anyhow::Result;
use event::Event;
use futures_util::{Sink, Stream, TryStreamExt};
use tap::TapFallible;
use tracing::{error, info};
use uuid::Uuid;

use crate::file_event_produce::WatchControl;
use crate::index::{Index, IndexFile, IndexGuard};
use crate::sync_control::rumors_event_handler::RumorsEventHandler;
use crate::sync_control::sync_all_handler::SyncAllHandler;
use crate::sync_control::watch_event_handler::WatchEventHandler;
use crate::transfer::DownloadTransfer;

pub mod event;
mod rumors_event_handler;
mod sync_all_handler;
mod watch_event_handler;

#[derive(Debug, Eq, PartialEq)]
pub struct SendRumors {
    pub dir_id: Uuid,
    pub rumors: Vec<IndexFile>,
    pub except: Option<Uuid>,
}

#[derive(Debug)]
pub struct SyncController<I, St, Si, Dl, Wc> {
    user_id: Uuid,
    dir_id: Uuid,
    sync_dir: PathBuf,
    index: I,
    event_stream: St,
    rumor_sender: Si,
    download_transfer: Dl,
    watch_control: Wc,
}

impl<I, St, Si, Dl, Wc> SyncController<I, St, Si, Dl, Wc> {
    pub fn new(
        user_id: Uuid,
        dir_id: Uuid,
        sync_dir: PathBuf,
        index: I,
        event_stream: St,
        rumor_sender: Si,
        download_transfer: Dl,
        watch_control: Wc,
    ) -> Self {
        Self {
            user_id,
            dir_id,
            sync_dir,
            index,
            event_stream,
            rumor_sender,
            download_transfer,
            watch_control,
        }
    }
}

impl<'a, I, St, Si, Dl, Wc, E1, E2> SyncController<I, St, Si, Dl, Wc>
where
    I: Index,
    <I::Guard as IndexGuard>::Error: Send + Sync + 'static,
    E1: Error + Send + Sync + 'static,
    St: Stream<Item = Result<Event, E1>> + Unpin,
    Si: Sink<SendRumors> + Unpin,
    Si::Error: Error + Send + Sync + 'static,
    Dl: DownloadTransfer + 'a,
    Dl::BlockStream<'a>: Unpin,
    Dl::Error: Into<io::Error>,
    Wc: WatchControl<Error = E2>,
    E2: Error + Send + Sync + 'static,
{
    pub async fn run(&mut self) -> Result<()> {
        while let Some(event) = self
            .event_stream
            .try_next()
            .await
            .tap_err(|err| error!(%err, "try next event failed"))?
        {
            self.pause_watch().await?;

            info!("pause watch done");

            match event {
                Event::Watch(watch_events) => {
                    let handler = WatchEventHandler::new(
                        &self.user_id,
                        &self.dir_id,
                        &self.sync_dir,
                        &self.index,
                        &mut self.rumor_sender,
                    );

                    handler.handle_watch_events(watch_events).await?;

                    info!("handle watch events done");
                }

                Event::Rumors {
                    sender_id,
                    remote_index: rumors,
                } => {
                    let rumors_event_handler = RumorsEventHandler::new(
                        self.user_id,
                        self.dir_id,
                        &self.sync_dir,
                        &self.index,
                        &self.download_transfer,
                        &mut self.rumor_sender,
                    );

                    rumors_event_handler
                        .handle_rumors_event(sender_id, rumors)
                        .await?;

                    info!("handle rumors events done");
                }

                Event::SyncAll => {
                    let sync_all_handler = SyncAllHandler::new(
                        &self.user_id,
                        &self.dir_id,
                        &self.sync_dir,
                        &self.index,
                        &mut self.rumor_sender,
                    );

                    sync_all_handler.handle_sync_all_event().await?;

                    info!("handle sync all event done");
                }
            }

            self.resume_watch().await?;

            info!("resume watch done");
        }

        info!(dir = ?self.sync_dir,"no more dir file watch event, stop sync");

        Ok(())
    }
}

impl<I, St, Si, Dl, Wc, E> SyncController<I, St, Si, Dl, Wc>
where
    Wc: WatchControl<Error = E>,
    E: Error + Send + Sync + 'static,
{
    #[inline]
    async fn pause_watch(&mut self) -> Result<()> {
        self.watch_control.pause_watch().await?;

        Ok(())
    }

    #[inline]
    async fn resume_watch(&mut self) -> Result<()> {
        self.watch_control.resume_watch().await?;

        Ok(())
    }
}
