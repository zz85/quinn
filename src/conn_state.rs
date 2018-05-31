use bytes::Buf;

use rand::{thread_rng, Rng};

use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;

use super::{QuicError, QuicResult, QUIC_VERSION};
use codec::{BufLen, Codec};
use crypto::Secret;
use frame::{Ack, AckFrame, CloseFrame, Frame, PaddingFrame, PathFrame, StreamFrame};
use packet::{Header, LongType, PartialDecode, ShortType};
use parameters::{ClientTransportParameters, ServerTransportParameters, TransportParameters};
use streams::Streams;
use tls;
use types::{ConnectionId, Side, GENERATED_CID_LENGTH};

pub struct ConnectionState<T> {
    side: Side,
    state: State,
    local: PeerData,
    remote: PeerData,
    src_pn: u32,
    secret: Secret,
    prev_secret: Option<Secret>,
    pub streams: Streams,
    queue: VecDeque<Vec<u8>>,
    control: VecDeque<Frame>,
    tls: T,
}

impl<T> ConnectionState<T>
where
    T: tls::Session + tls::QuicSide,
{
    pub fn new(tls: T, secret: Option<Secret>) -> Self {
        let mut rng = thread_rng();
        let dst_cid = rng.gen();
        let side = tls.side();

        let secret = if side == Side::Client {
            debug_assert!(secret.is_none());
            Secret::Handshake(dst_cid)
        } else if let Some(secret) = secret {
            secret
        } else {
            panic!("need secret for client conn_state");
        };

        let local = PeerData::new(rng.gen());
        let (num_recv_bidi, num_recv_uni) = (
            u64::from(local.params.max_streams_bidi),
            u64::from(local.params.max_stream_id_uni),
        );
        let (max_recv_bidi, max_recv_uni) = if side == Side::Client {
            (1 + 4 * num_recv_bidi, 3 + 4 * num_recv_uni)
        } else {
            (4 * num_recv_bidi, 1 + 4 * num_recv_uni)
        };

        let mut streams = Streams::new(side);
        streams.update_max_id(max_recv_bidi);
        streams.update_max_id(max_recv_uni);

        ConnectionState {
            tls,
            side,
            state: State::Start,
            remote: PeerData::new(dst_cid),
            local,
            src_pn: rng.gen(),
            secret,
            prev_secret: None,
            streams,
            queue: VecDeque::new(),
            control: VecDeque::new(),
        }
    }

    pub fn is_handshaking(&self) -> bool {
        match self.state {
            State::Connected => false,
            _ => true,
        }
    }

    pub fn queued(&mut self) -> QuicResult<Option<&Vec<u8>>> {
        let mut payload = vec![];
        while let Some(frame) = self.control.pop_front() {
            payload.push(frame);
        }
        while let Some(frame) = self.streams.queued() {
            payload.push(frame);
        }

        if !payload.is_empty() {
            self.queue_packet(payload)?;
        }
        Ok(self.queue.front())
    }

    pub fn pop_queue(&mut self) {
        self.queue.pop_front();
    }

    pub fn pick_unused_cid<F>(&mut self, is_used: F) -> ConnectionId
    where
        F: Fn(ConnectionId) -> bool,
    {
        while is_used(self.local.cid) {
            self.local.cid = thread_rng().gen();
        }
        self.local.cid
    }

    pub(crate) fn set_secret(&mut self, secret: Secret) {
        let old = mem::replace(&mut self.secret, secret);
        self.prev_secret = Some(old);
    }

    #[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
    pub fn queue_packet(&mut self, mut payload: Vec<Frame>) -> QuicResult<()> {
        let ptype = match self.state {
            State::Connected => None,
            State::Handshaking => Some(LongType::Handshake),
            State::InitialSent => {
                self.state = State::Handshaking;
                Some(LongType::Handshake)
            }
            State::Start => {
                if self.side == Side::Client {
                    self.state = State::InitialSent;
                    Some(LongType::Initial)
                } else {
                    self.state = State::Handshaking;
                    Some(LongType::Handshake)
                }
            }
            State::FinalHandshake => {
                self.state = State::Connected;
                Some(LongType::Handshake)
            }
        };

        let secret = if let Some(LongType::Handshake) = ptype {
            if let Some(ref secret @ Secret::Handshake(_)) = self.prev_secret {
                secret
            } else {
                &self.secret
            }
        } else {
            &self.secret
        };
        let key = secret.build_key(self.side);
        let tag_len = key.algorithm().tag_len();

        let mut encrypted_payload_len = (payload.buf_len() + tag_len) as u64;
        if ptype == Some(LongType::Initial) && encrypted_payload_len < 1200 {
            payload.push(Frame::Padding(PaddingFrame((1200 - encrypted_payload_len) as usize)));
            encrypted_payload_len = 1200;
        }

        let number = self.src_pn;
        self.src_pn += 1;
        let (dst_cid, src_cid) = (self.remote.cid, self.local.cid);
        debug_assert_eq!(src_cid.len, GENERATED_CID_LENGTH);
        let header = match ptype {
            Some(ltype) => Header::Long {
                ptype: ltype,
                version: QUIC_VERSION,
                dst_cid,
                src_cid,
                len: encrypted_payload_len,
                number,
            },
            None => Header::Short {
                key_phase: false,
                ptype: ShortType::Two,
                dst_cid,
                number,
            },
        };

        debug_assert_eq!(tag_len, self.secret.tag_len());
        let buf = vec![0u8; header.buf_len() + (encrypted_payload_len as usize)];
        let (header_len, msg_len, mut buf) = {
            let mut write = Cursor::new(buf);
            header.encode(&mut write);
            let header_len = write.position() as usize;
            debug_assert_eq!(header_len, header.buf_len());

            for frame in &payload {
                let before = write.position();
                frame.encode(&mut write);
                let after = write.position();
                debug_assert_eq!((after - before) as usize, frame.buf_len());
            }
            let msg_len = write.position() as usize;
            (header_len, msg_len, write.into_inner())
        };

        let out_len = {
            let (header_buf, mut payload) = buf.split_at_mut(header_len);
            let mut in_out = &mut payload[..msg_len - header_len + tag_len];
            key.encrypt(number, &header_buf, in_out, tag_len)?
        };

        debug_assert_eq!(encrypted_payload_len, out_len as u64);
        self.queue.push_back(buf);
        Ok(())
    }

    pub(crate) fn handle(&mut self, buf: &mut [u8]) -> QuicResult<()> {
        self.handle_partial(PartialDecode::new(buf)?)
    }

    pub(crate) fn handle_partial(&mut self, partial: PartialDecode) -> QuicResult<()> {
        let PartialDecode {
            header,
            header_len,
            buf,
        } = partial;

        let key = {
            let secret = if let Some(LongType::Handshake) = header.ptype() {
                if let Some(ref secret @ Secret::Handshake(_)) = self.prev_secret {
                    secret
                } else {
                    &self.secret
                }
            } else {
                &self.secret
            };
            secret.build_key(self.side.other())
        };

        let payload = match header {
            Header::Long { number, .. } | Header::Short { number, .. } => {
                let (header_buf, payload_buf) = buf.split_at_mut(header_len);
                let decrypted = key.decrypt(number, &header_buf, payload_buf)?;
                let mut read = Cursor::new(decrypted);

                let mut payload = Vec::new();
                while read.has_remaining() {
                    let frame = Frame::decode(&mut read)?;
                    payload.push(frame);
                }
                payload
            }
            Header::Negotiation { .. } => vec![],
        };

        self.handle_packet(header, payload)
    }

    #[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
    fn handle_packet(&mut self, header: Header, payload: Vec<Frame>) -> QuicResult<()> {
        let (dst_cid, number) = match header {
            Header::Long {
                dst_cid,
                src_cid,
                number,
                ..
            } => match self.state {
                State::Start | State::InitialSent => {
                    self.remote.cid = src_cid;
                    (dst_cid, number)
                }
                _ => (dst_cid, number),
            },
            Header::Short {
                dst_cid, number, ..
            } => if let State::Connected = self.state {
                (dst_cid, number)
            } else {
                return Err(QuicError::General(format!(
                    "{:?} received short header in {:?} state",
                    self.side, self.state
                )));
            },
            Header::Negotiation { .. } => {
                return Err(QuicError::General(format!(
                    "negotiation packet not handled by connections"
                )));
            }
        };

        if self.state != State::Start && dst_cid != self.local.cid {
            return Err(QuicError::General(format!(
                "invalid destination CID {:?} received (expected {:?})",
                dst_cid, self.local.cid
            )));
        }

        self.control.push_back(Frame::Ack(AckFrame {
            largest: number,
            ack_delay: 0,
            blocks: vec![Ack::Ack(0)],
        }));

        let mut received_tls = false;
        for frame in &payload {
            match frame {
                Frame::Stream(f) if f.id == 0 => {
                    received_tls = true;
                    self.handle_tls(Some(f))?;
                }
                Frame::PathChallenge(PathFrame(token)) => {
                    self.control.push_back(Frame::PathResponse(PathFrame(*token)));
                }
                Frame::ApplicationClose(CloseFrame { code, reason }) => {
                    return Err(QuicError::ApplicationClose(*code, reason.clone()));
                }
                Frame::ConnectionClose(CloseFrame { code, reason }) => {
                    return Err(QuicError::ConnectionClose(*code, reason.clone()));
                }
                Frame::Ack(_)
                | Frame::Padding(_)
                | Frame::PathResponse(_)
                | Frame::Stream(_)
                | Frame::Ping
                | Frame::StreamIdBlocked(_) => {}
            }
        }

        if let (State::Handshaking, false) = (&self.state, received_tls) {
            self.handle_tls(None)?;
        }
        Ok(())
    }

    fn handle_tls(&mut self, frame: Option<&StreamFrame>) -> QuicResult<()> {
        let (handshake, new_secret) =
            tls::process_handshake_messages(&mut self.tls, frame.map(|f| f.data.as_ref()))?;

        if let Some(secret) = new_secret {
            self.set_secret(secret);
            self.state = match self.side {
                Side::Client => State::FinalHandshake,
                Side::Server => State::Connected,
            };

            let params = match self.tls.get_quic_transport_parameters() {
                None => {
                    return Err(QuicError::General(
                        "no transport parameters received".into(),
                    ));
                }
                Some(bytes) => {
                    let mut read = Cursor::new(bytes);
                    if self.side == Side::Client {
                        ServerTransportParameters::decode(&mut read)?.parameters
                    } else {
                        ClientTransportParameters::decode(&mut read)?.parameters
                    }
                }
            };

            mem::replace(&mut self.remote.params, params);

            let (num_send_bidi, num_send_uni) = (
                u64::from(self.remote.params.max_streams_bidi),
                u64::from(self.remote.params.max_stream_id_uni),
            );
            let (max_send_bidi, max_send_uni) = if self.side == Side::Server {
                (1 + 4 * num_send_bidi, 3 + 4 * num_send_uni)
            } else {
                (4 * num_send_bidi, 1 + 4 * num_send_uni)
            };
            self.streams.update_max_id(max_send_bidi);
            self.streams.update_max_id(max_send_uni);
        }

        let mut stream = self.streams
            .received(0)
            .ok_or_else(|| QuicError::General("no incoming packets allowed on stream 0".into()))?;
        let offset = stream.get_offset();
        stream.set_offset(offset + handshake.len() as u64);

        if !handshake.is_empty() {
            self.control.push_back(Frame::Stream(StreamFrame {
                id: 0,
                fin: false,
                offset,
                len: Some(handshake.len() as u64),
                data: handshake,
            }));
        }
        Ok(())
    }
}

impl ConnectionState<tls::ClientSession> {
    pub(crate) fn initial(&mut self) -> QuicResult<()> {
        self.handle_tls(None)
    }
}

pub struct PeerData {
    pub cid: ConnectionId,
    pub params: TransportParameters,
}

impl PeerData {
    pub fn new(cid: ConnectionId) -> Self {
        PeerData {
            cid,
            params: TransportParameters::default(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum State {
    Start,
    InitialSent,
    Handshaking,
    FinalHandshake,
    Connected,
}

#[cfg(test)]
pub mod tests {
    use super::{ClientTransportParameters, ConnectionId, ServerTransportParameters};
    use super::{tls, ConnectionState, PartialDecode, Secret};
    use std::sync::Arc;

    #[test]
    fn test_encoded_handshake() {
        let mut c = client_conn_state();
        c.initial().unwrap();
        let mut cp = c.queued().unwrap().unwrap().clone();
        c.pop_queue();

        let mut s = server_conn_state(PartialDecode::new(&mut cp).unwrap().dst_cid());
        s.handle(&mut cp).unwrap();
        let mut sp = s.queued().unwrap().unwrap().clone();
        s.pop_queue();

        let mut rt = 10;
        loop {
            c.handle(&mut sp).unwrap();
            cp = c.queued().unwrap().unwrap().clone();
            c.pop_queue();

            s.handle(&mut cp).unwrap();
            sp = s.queued().unwrap().unwrap().clone();
            s.pop_queue();

            let header = PartialDecode::new(&mut sp).unwrap().header;
            if header.ptype().is_none() {
                break;
            }

            rt -= 1;
            if rt < 1 {
                panic!("short header not emitted within 10 round trips");
            }
        }
    }

    #[test]
    fn test_handshake() {
        let mut c = client_conn_state();
        c.initial().unwrap();
        let mut initial = c.queued().unwrap().unwrap().clone();
        c.pop_queue();

        let mut s = server_conn_state(PartialDecode::new(&mut initial).unwrap().dst_cid());
        s.handle(&mut initial).unwrap();
        let mut server_hello = s.queued().unwrap().unwrap().clone();

        c.handle(&mut server_hello).unwrap();
        assert!(c.queued().unwrap().is_some());
    }

    pub fn server_conn_state(hs_cid: ConnectionId) -> ConnectionState<tls::ServerSession> {
        ConnectionState::new(
            tls::server_session(
                &Arc::new(tls::tests::server_config()),
                &ServerTransportParameters::default(),
            ),
            Some(Secret::Handshake(hs_cid)),
        )
    }

    pub fn client_conn_state() -> ConnectionState<tls::ClientSession> {
        ConnectionState::new(
            tls::client_session(
                Some(tls::tests::client_config()),
                "Localhost",
                &ClientTransportParameters::default(),
            ).unwrap(),
            None,
        )
    }
}