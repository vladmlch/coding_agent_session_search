//! A process-owned hard deadline also bounds non-cooperative native engine calls.
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

enum Message {
    Begin(Instant),
    Done,
    Stop,
}

pub struct Watchdog {
    sender: Sender<Message>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    pub fn start() -> std::io::Result<Self> {
        let (sender, receiver) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("viewer-bridge-deadline".into())
            .spawn(move || {
                let mut deadline: Option<Instant> = None;
                loop {
                    let message = match deadline {
                        Some(end) => match receiver
                            .recv_timeout(end.saturating_duration_since(Instant::now()))
                        {
                            Ok(value) => value,
                            Err(RecvTimeoutError::Disconnected) => return,
                            Err(RecvTimeoutError::Timeout) => {
                                eprintln!("cass-viewer-bridge: request hard deadline exceeded");
                                std::process::exit(70);
                            }
                        },
                        None => match receiver.recv() {
                            Ok(value) => value,
                            Err(_) => return,
                        },
                    };
                    match message {
                        Message::Begin(end) => deadline = Some(end),
                        Message::Done => deadline = None,
                        Message::Stop => return,
                    }
                }
            })?;
        Ok(Self {
            sender,
            thread: Some(thread),
        })
    }

    pub fn begin(&self, timeout_ms: u64) {
        // The cooperative deadline produces typed refusals; a 250 ms cleanup
        // margin permits serialization/rollback before the process hard stop.
        let _ = self.sender.send(Message::Begin(
            Instant::now() + Duration::from_millis(timeout_ms + 250),
        ));
    }

    pub fn done(&self) {
        let _ = self.sender.send(Message::Done);
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.sender.send(Message::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
