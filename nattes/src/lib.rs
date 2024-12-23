// #![warn(missing_docs)]

//!Simple subject-based message queue inspired by nats

#[cfg(test)]
mod tests;

use bytes::Bytes;
use futures::Stream;
use std::fmt::Display;
use std::{
    collections::HashMap,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use uuid::Uuid;

///Nattes is a clonable instance of the message queue
#[derive(Clone, Default)]
pub struct Nattes {
    subscribers: Arc<Mutex<HashMap<Uuid, SubscriberHandle>>>,
}

///SubscriberHandle is the internal reference of a subscriber
#[derive(Clone)]
pub struct SubscriberHandle {
    subscribe_subject: SubscribeSubject,
    sender: Sender<Bytes>,
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

impl From<PublishSubject> for SubscribeSubject {
    fn from(value: PublishSubject) -> Self {
        Self(value.0)
    }
}

impl Display for PublishSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

impl From<PublishSubject> for String {
    fn from(value: PublishSubject) -> Self {
        value.0.join(".")
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
            if part.contains('*') && part != "*" {
                return Err(NatsError::InvalidSubscribeSubject);
            }

            // Match any part (to the end)
            if part.contains('>') && part != ">" || i != subject_parts.len() - 1 {
                return Err(NatsError::InvalidSubscribeSubject);
            }

            result.push(part.into());
        }
        Ok(SubscribeSubject(result))
    }
}

impl Display for SubscribeSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

impl From<SubscribeSubject> for String {
    fn from(val: SubscribeSubject) -> Self {
        val.0.join(".")
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
            ..Default::default()
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

        for handle in list.values() {
            if handle.subscribe_subject.check_subject(&subject) {
                handle.sender.send(message.clone()).await?
            }
        }

        Ok(())
    }

    pub async fn subscribe(&self, subject: SubscribeSubject) -> Result<Subscriber, NatsError> {
        let (tx, rx) = channel(100);
        let uuid = Uuid::now_v7();
        let handle = SubscriberHandle {
            subscribe_subject: subject.clone(),
            sender: tx,
        };

        {
            let mut lock = self.subscribers.lock().unwrap();
            lock.insert(uuid, handle);
        }

        Ok(Subscriber {
            uuid,
            subject,
            channel: rx,
            nattes: Nattes::new(),
        })
    }

    pub async fn request(_subject: PublishSubject, _message: impl Into<Bytes>) {
        unimplemented!()
    }

    pub async fn reply(_subject: SubscribeSubject) {
        unimplemented!()
    }

    pub(self) fn unsubscribe(&self, uuid: Uuid) {
        {
            let mut lock = self.subscribers.lock().unwrap();
            lock.remove(&uuid);
        }
    }
}

pub struct Subscriber {
    uuid: Uuid,
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

impl Subscriber {
    pub async fn unsubscribe(self) {
        drop(self)
    }

    pub fn subject(&self) -> SubscribeSubject {
        self.subject.clone()
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.nattes.unsubscribe(self.uuid)
    }
}
