use std::{
    any::Any,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Buf;
use futures::{ready, Stream as _};
use http_body::Body;
use quinn::SendStream;
use quinn_proto::StreamId;

use crate::{
    body::RecvBody,
    connection::ConnectionRef,
    frame::{FrameStream, WriteFrame},
    headers::{DecodeHeaders, SendHeaders},
    proto::{
        frame::{DataFrame, HttpFrame},
        headers::Header,
        ErrorCode,
    },
    streams::Reset,
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

impl<B> Future for SendData<B, B::Data>
where
    B: Body + Unpin,
    B::Data: Buf + Unpin,
    B::Error: std::fmt::Debug + Any + Send + Sync,
{
    type Output = Result<(), Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        while !ready!(self.poll_inner(cx))? {}
        Poll::Ready(Ok(()))
    }
}

pub struct RecvData {
    state: RecvDataState,
    conn: ConnectionRef,
    recv: Option<FrameStream>,
    stream_id: StreamId,
}

enum RecvDataState {
    Receiving,
    Decoding(DecodeHeaders),
    Finished,
}

impl RecvData {
    pub(crate) fn new(recv: FrameStream, conn: ConnectionRef, stream_id: StreamId) -> Self {
        Self {
            conn,
            stream_id,
            recv: Some(recv),
            state: RecvDataState::Receiving,
        }
    }

    pub fn reset(&mut self, err_code: ErrorCode) {
        if let Some(ref mut r) = self.recv {
            r.reset(err_code.into());
        }
    }
}

impl Future for RecvData {
    type Output = Result<(Header, RecvBody), Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                RecvDataState::Receiving => {
                    match ready!(Pin::new(self.recv.as_mut().unwrap()).poll_next(cx)) {
                        Some(Ok(HttpFrame::Reserved)) => continue,
                        Some(Ok(HttpFrame::Headers(h))) => {
                            self.state = RecvDataState::Decoding(DecodeHeaders::new(
                                h,
                                self.conn.clone(),
                                self.stream_id,
                            ));
                        }
                        Some(Err(e)) => {
                            self.recv.as_mut().unwrap().reset(e.code());
                            return Poll::Ready(Err(e.into()));
                        }
                        Some(Ok(f)) => {
                            self.recv
                                .as_mut()
                                .unwrap()
                                .reset(ErrorCode::FRAME_UNEXPECTED);
                            return Poll::Ready(Err(Error::Peer(format!(
                                "First frame is not headers: {:?}",
                                f
                            ))));
                        }
                        None => {
                            return Poll::Ready(Err(Error::peer("Stream end unexpected")));
                        }
                    };
                }
                RecvDataState::Decoding(ref mut decode) => {
                    let headers = ready!(Pin::new(decode).poll(cx))?;
                    let recv =
                        RecvBody::new(self.conn.clone(), self.stream_id, self.recv.take().unwrap());
                    self.state = RecvDataState::Finished;
                    return Poll::Ready(Ok((headers, recv)));
                }
                RecvDataState::Finished => panic!("polled after finished"),
            }
        }
    }
}
