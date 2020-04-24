use std::{
    any::Any,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Buf;
use futures::ready;
use http_body::Body;
use quinn::SendStream;
use quinn_proto::StreamId;

use crate::{
    connection::ConnectionRef,
    frame::WriteFrame,
    headers::SendHeaders,
    proto::{
        frame::DataFrame,
        headers::Header,
        ErrorCode,
    },
    Error,
};

/// Represent data transmission completion for a Request or a Response
///
/// This is yielded by [`SendRequest`] and [`SendResponse`]. It will encode and send
/// the headers, then send the body if any data is polled from [`HttpBody::poll_data()`].
/// It also encodes and sends the trailer a similar way, if any.
pub struct SendData<B, P> {
    headers: Option<Header>,
    body: Box<B>,
    state: SendDataState<P>,
    conn: ConnectionRef,
    send: Option<SendStream>,
    stream_id: StreamId,
}

enum SendDataState<P> {
    Initial,
    Headers(SendHeaders),
    PollBody,
    Write(WriteFrame<DataFrame<P>>),
    PollTrailers,
    Trailers(SendHeaders),
    Finished,
}

impl<B> SendData<B, B::Data>
where
    B: Body + Unpin,
    B::Data: Buf + Unpin,
    B::Error: std::fmt::Debug + Any + Send + Sync,
{
    pub(crate) fn new(send: SendStream, conn: ConnectionRef, headers: Header, body: B) -> Self {
        Self {
            conn,
            headers: Some(headers),
            stream_id: send.id(),
            send: Some(send),
            state: SendDataState::Initial,
            body: Box::new(body),
        }
    }

    /// Cancel the request
    ///
    /// The peer will receive a request error with `REQUEST_CANCELLED` code.
    pub fn cancel(&mut self) {
        self.state = SendDataState::Finished;
        match self.state {
            SendDataState::Write(ref mut w) => {
                w.reset(ErrorCode::REQUEST_CANCELLED);
            }
            SendDataState::Trailers(ref mut w) => {
                w.reset(ErrorCode::REQUEST_CANCELLED);
            }
            _ => {
                if let Some(ref mut send) = self.send.take() {
                    send.reset(ErrorCode::REQUEST_CANCELLED.into());
                }
            }
        }
        self.state = SendDataState::Finished;
    }

    fn poll_inner(&mut self, cx: &mut Context) -> Poll<Result<bool, Error>> {
        match &mut self.state {
            SendDataState::Initial => {
                // This initial computaion is done here to report its failability to Future::Output.
                let header = self.headers.take().expect("headers");
                self.state = SendDataState::Headers(self.send_header(header)?);
            }
            SendDataState::Headers(ref mut send) => {
                self.send = Some(ready!(Pin::new(send).poll(cx))?);
                self.state = SendDataState::PollBody;
            }
            SendDataState::PollBody => {
                let data = ready!(Pin::new(self.body.as_mut()).poll_data(cx));
                match data {
                    None => self.state = SendDataState::PollTrailers,
                    Some(Err(e)) => return Poll::Ready(Err(Error::body(e))),
                    Some(Ok(d)) => {
                        let send = self.send.take().expect("send");
                        let data = DataFrame { payload: d };
                        self.state = SendDataState::Write(WriteFrame::new(send, data));
                    }
                }
            }
            SendDataState::Write(ref mut write) => {
                self.send = Some(ready!(Pin::new(write).poll(cx))?);
                self.state = SendDataState::PollBody;
            }
            SendDataState::PollTrailers => {
                let data = ready!(Pin::new(self.body.as_mut()).poll_trailers(cx)).expect("TODO");
                match data {
                    // TODO finish
                    None => return Poll::Ready(Ok(true)),
                    // TODO finish
                    Some(h) => {
                        self.state = SendDataState::Trailers(self.send_header(Header::trailer(h))?);
                    }
                }
            }
            SendDataState::Trailers(send) => {
                ready!(Pin::new(send).poll(cx))?;
                self.state = SendDataState::Finished;
                return Poll::Ready(Ok(true));
            }
            SendDataState::Finished => return Poll::Ready(Ok(true)),
        }

        Poll::Ready(Ok(false))
    }

    fn send_header(&mut self, header: Header) -> Result<SendHeaders, Error> {
        Ok(SendHeaders::new(
            header,
            &self.conn,
            self.send.take().expect("send"),
            self.stream_id,
        )?)
    }
}
