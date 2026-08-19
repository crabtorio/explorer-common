/// A wrapper around [`crossbeam_channel`] channels that provides automatic logging of message events
pub struct LoggedChannel<SendT, RecvT> {
    reciever: crossbeam_channel::Receiver<RecvT>,
    sender: crossbeam_channel::Sender<SendT>,
    reciever_ident: String,
}
pub enum ChannelError<T> {
    SendError(crossbeam_channel::SendError<T>),
    RecvError(crossbeam_channel::RecvError),
    InvalidResponseError,
}

impl<SendT: std::fmt::Debug, RecvT: std::fmt::Debug> LoggedChannel<SendT, RecvT> {
    /// Send a message.
    /// Returns a the message, or [`crossbeam_channel::RecvError`] if an error occurs.
    /// ---
    /// Logs the folliwing events:
    /// - Send
    /// - Send errors
    pub fn send(&self, val: SendT) -> Result<(), crossbeam_channel::SendError<SendT>> {
        let val_debug = std::format!("{:?}", val);
        let result = self.sender.send(val);

        if result.is_ok() {
            log::debug!("{} sent to {}", val_debug, self.reciever_ident)
        } else {
            log::error!("Could not send {} to {}", val_debug, self.reciever_ident)
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
                log::debug!("Recieved {:?} from {}", val, self.reciever_ident);
                Ok(val)
            }
            Err(err) => {
                log::error!(
                    "{} error while waiting on response from {}",
                    err,
                    self.reciever_ident
                );
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
        let send_result = self.send(val);
        match send_result {
            Ok(()) => match self.recv() {
                Ok(res) => {
                    let res_debug = std::format!("{:?}", res);
                    if ack_to_ckeck != res.into() {
                        log::error!(
                            "Invalid response from {:?}. Expected {:?}, got {}",
                            self.reciever_ident,
                            ack_to_ckeck,
                            res_debug
                        );
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
        log::trace!("Polling {:?}...", self.reciever_ident);
        match self.reciever.try_recv() {
            Ok(val) => {
                log::debug!("Recieved {:?} from {}", val, self.reciever_ident);
                Ok(Some(val))
            }
            Err(err) => match err {
                crossbeam_channel::TryRecvError::Empty => {
                    log::trace!("Polled {:?}, got no response", self.reciever_ident);
                    Ok(None)
                }
                crossbeam_channel::TryRecvError::Disconnected => {
                    log::error!(
                        "{:?} disconnected unexpectedly while being polled",
                        self.reciever_ident
                    );
                    Err(())
                }
            },
        }
    }
}
