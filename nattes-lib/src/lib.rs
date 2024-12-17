// #![warn(missing_docs)]

//!Simple subject-based message queue inspired by nats

#[cfg(test)]
mod tests;

use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver, Sender};

///Nattes is a clonable instance of the message queue
pub struct Nattes {
    subscribers: Arc<Mutex<Vec<(SubscribeSubject, Sender<Bytes>)>>>,
}

#[derive(Debug, Error)]
pub enum NatsError {
    #[error("Invalid publish subject")]
    InvalidPublishSubject,
    #[error("Invalid subscribe subject")]
    InvalidSubscribeSubject,

    #[error("Failed to send message")]
    MpscError(#[from] tokio::sync::mpsc::error::SendError<Bytes>),
}

#[derive(Debug, Clone)]
pub struct PublishSubject(Vec<String>);

impl FromStr for PublishSubject {
    type Err = NatsError;
    fn from_str(subject: &str) -> Result<Self, Self::Err> {
        if "*>\0 ".chars().any(|a| subject.contains(a)) {
            return Err(NatsError::InvalidPublishSubject);
        }
        Ok(PublishSubject(
            subject.split('.').map(|s| s.to_owned()).collect(),
        ))
    }
}

impl Into<SubscribeSubject> for PublishSubject {
    fn into(self) -> SubscribeSubject {
        SubscribeSubject(self.0)
    }
}
#[derive(Debug, Clone)]
pub struct SubscribeSubject(Vec<String>);

impl FromStr for SubscribeSubject {
    type Err = NatsError;
    fn from_str(subject: &str) -> Result<Self, Self::Err> {
        if "\0 ".chars().any(|a| subject.contains(a)) {
            return Err(NatsError::InvalidPublishSubject);
        }
        let subject_parts: Vec<String> = subject.split('.').map(|a| a.to_string()).collect();
        let mut result: Vec<String> = Vec::new();
        for i in 0..subject_parts.len() {
            let part = &subject_parts[i];

            // Match any part (one time)
            if part.contains('*') {
                if part != "*" {
                    return Err(NatsError::InvalidSubscribeSubject);
                }
            }

            // Match any part (to the end)
            if part.contains('>') {
                if part != ">" || i != subject_parts.len() - 1 {
                    return Err(NatsError::InvalidSubscribeSubject);
                }
            }

            result.push(part.into());
        }
        Ok(SubscribeSubject(result))
    }
}

impl SubscribeSubject {
    pub fn check_subject(&self, subject: &PublishSubject) -> bool {
        for i in 0..self.0.len() {
            if self.0[i] == ">" && subject.0.get(i).is_some() {
                return true;
            }

            if self.0[i] == "*" && subject.0.get(i).is_some() {
                continue;
            }

            if self.0.get(i) != subject.0.get(i) {
                return false;
            }
        }
        self.0.len() == subject.0.len()
    }
}
impl Nattes {
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn publish(
        &self,
        subject: PublishSubject,
        message: impl Into<Bytes>,
    ) -> Result<(), NatsError> {
        let list = {
            let lock = self.subscribers.lock().unwrap();
            lock.clone()
        };

        let message = message.into();

        for (sub, tx) in list {
            if (sub.check_subject(&subject)) {
                tx.send(message.clone()).await?
            }
        }

        Ok(())
    }

    pub async fn subscribe(&self, subject: SubscribeSubject) -> Result<Subscriber, NatsError> {
        let (tx, rx) = channel(100);
        {
            let mut lock = self.subscribers.lock().unwrap();
            lock.push((subject.clone(), tx));
        }

        Ok(Subscriber {
            subject,
            channel: rx,
            nattes: Nattes::new(),
        })
    }

    pub async fn request(subject: PublishSubject, message: impl Into<Bytes>) {
        unimplemented!()
    }

    pub async fn reply(subject: SubscribeSubject) {
        unimplemented!()
    }
}

pub struct Subscriber {
    subject: SubscribeSubject,
    channel: Receiver<Bytes>,
    nattes: Nattes,
}

impl Stream for Subscriber {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.channel.poll_recv(cx)
    }
}
