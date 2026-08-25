use common_game::logging::{Channel, EventType, LogEvent, Participant, Payload};
use std::format;

/// A wrapper around [`crossbeam_channel`] channels that provides automatic logging of message events
pub struct LoggedChannel<SendT, RecvT> {
    reciever: crossbeam_channel::Receiver<RecvT>,
    sender: crossbeam_channel::Sender<SendT>,
    send_participant: Participant,
    recv_participant: Participant,
    send_event: EventType,
    recv_event: EventType,
}
pub enum ChannelError<T> {
    SendError(crossbeam_channel::SendError<T>),
    RecvError(crossbeam_channel::RecvError),
    InvalidResponseError,
}

impl<SendT, RecvT> Clone for LoggedChannel<SendT, RecvT> {
    fn clone(&self) -> Self {
        Self {
            reciever: self.reciever.clone(),
            sender: self.sender.clone(),
            send_participant: self.send_participant.clone(),
            recv_participant: self.recv_participant.clone(),
            send_event: self.send_event.clone(),
            recv_event: self.recv_event.clone(),
        }
    }
}

enum Direction {
    Send,
    Recv,
}

impl<SendT: std::fmt::Debug, RecvT: std::fmt::Debug> LoggedChannel<SendT, RecvT> {
    pub fn new(
        reciever: crossbeam_channel::Receiver<RecvT>,
        sender: crossbeam_channel::Sender<SendT>,
        send_participant: Participant,
        recv_participant: Participant,
        send_event: EventType,
        recv_event: EventType,
    ) -> Self {
        Self {
            reciever,
            sender,
            send_participant,
            recv_participant,
            send_event,
            recv_event,
        }
    }

    fn make_log_event(&self, direction: Direction, channel: Channel, payload: Payload) -> LogEvent {
        LogEvent::new(
            Some(self.send_participant.clone()),
            Some(self.recv_participant.clone()),
            match direction {
                Direction::Send => self.send_event.clone(),
                Direction::Recv => self.recv_event.clone(),
            },
            channel,
            payload,
        )
    }

    /// Send a message.
    /// Returns a the message, or [`crossbeam_channel::RecvError`] if an error occurs.
    /// ---
    /// Logs the folliwing events:
    /// - Send
    /// - Send errors
    pub fn send(&self, val: SendT) -> Result<(), crossbeam_channel::SendError<SendT>> {
        let val_debug = format!("{:?}", val);
        let result = self.sender.send(val);
        if result.is_ok() {
            self.make_log_event(
                Direction::Send,
                Channel::Debug,
                Payload::from([("Message".into(), format!("Sent {val_debug}"))]),
            )
            .emit();
        } else {
            self.make_log_event(
                Direction::Send,
                Channel::Debug,
                Payload::from([("Message".into(), format!("Could not send {val_debug}"))]),
            )
            .emit();
        }

        return result;
    }

    /// Await a message.
    /// Returns a the message, or [`crossbeam_channel::RecvError`] if an error occurs.
    /// ---
    /// Logs the folliwing events:
    /// - Recv
    /// - Recv errors
    /// ---
    /// Respone handling rests entirely on the caller
    pub fn recv(&self) -> Result<RecvT, crossbeam_channel::RecvError> {
        let result = self.reciever.recv();
        match result {
            Ok(val) => {
                self.make_log_event(
                    Direction::Recv,
                    Channel::Debug,
                    Payload::from([("Message".into(), format!("Recieved {val:?}"))]),
                )
                .emit();
                Ok(val)
            }
            Err(err) => {
                self.make_log_event(
                    Direction::Recv,
                    Channel::Error,
                    Payload::from([(
                        "Message".into(),
                        format!("Got error while awaiting response {err:?}"),
                    )]),
                )
                .emit();
                Err(err)
            }
        }
    }

    /// Send a message, then await and check the validity of the response.
    /// Returns a [`ChannelError`] when an error occurs internally.
    /// ---
    /// Logs the folliwing events:
    //  - Send
    /// - Send errors
    /// - Recv
    /// - Recv errors
    /// - Invalid response
    /// ---
    /// Useful for simple messages.
    pub fn send_and_check_ack<T: PartialEq + std::fmt::Debug>(
        &self,
        val: SendT,
        ack_to_ckeck: T,
    ) -> Result<(), ChannelError<SendT>>
    where
        RecvT: Into<T>,
    {
        let val_debug = format!("{val:?}");
        let send_result = self.send(val);
        match send_result {
            Ok(()) => match self.recv() {
                Ok(res) => {
                    let res_debug = std::format!("{:?}", res);
                    if ack_to_ckeck != res.into() {
                        self.make_log_event(
                            Direction::Recv,
                            Channel::Error,
                            Payload::from([
                                ("Message".into(), "Recieved invalid response".into()),
                                ("Expected".into(), format!("{val_debug:?}")),
                                ("Got".into(), format!("{res_debug:?}")),
                            ]),
                        )
                        .emit();
                        Err(ChannelError::InvalidResponseError)
                    } else {
                        Ok(())
                    }
                }
                Err(err) => Err(ChannelError::RecvError(err)),
            },
            Err(err) => Err(ChannelError::SendError(err)),
        }
    }

    /// Poll the channel for incoming messages (non-blocking).
    /// Optionally returns the message (if any found), or `()` if the client disconnects.
    /// ---
    /// Logs the folliwing events:
    /// - Poll start
    /// - No response
    /// - Recv
    /// - Disconnect error
    /// ---
    /// Respone handling rests entirely on the caller
    pub fn poll(&self) -> Result<Option<RecvT>, ()> {
        self.make_log_event(
            Direction::Recv,
            Channel::Trace,
            Payload::from([("Message".into(), "Polling started".into())]),
        )
        .emit();
        match self.reciever.try_recv() {
            Ok(val) => {
                self.make_log_event(
                    Direction::Recv,
                    Channel::Debug,
                    Payload::from([("Message".into(), format!("Recieved {val:?}"))]),
                )
                .emit();
                Ok(Some(val))
            }
            Err(err) => match err {
                crossbeam_channel::TryRecvError::Empty => {
                    self.make_log_event(
                        Direction::Recv,
                        Channel::Trace,
                        Payload::from([("Message".into(), "Got no response in poll".into())]),
                    );
                    Ok(None)
                }
                crossbeam_channel::TryRecvError::Disconnected => {
                    self.make_log_event(
                        Direction::Recv,
                        Channel::Error,
                        Payload::from([(
                            "Message".into(),
                            "Peer disconnected while being polled".into(),
                        )]),
                    )
                    .emit();
                    Err(())
                }
            },
        }
    }
    pub fn set_sender(&mut self, sender: crossbeam_channel::Sender<SendT>) {
        self.sender = sender;
    }
    pub fn set_receiver(&mut self, receiver: crossbeam_channel::Receiver<RecvT>) {
        self.reciever = receiver;
    }
}
