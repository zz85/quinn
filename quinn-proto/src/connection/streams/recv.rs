use std::collections::hash_map::Entry;
use std::mem;

use thiserror::Error;
use tracing::debug;

use super::{Retransmits, ShouldTransmit, StreamHalf, StreamId, Streams, UnknownStream};
use crate::connection::assembler::{Assembler, Chunk, IllegalOrderedRead};
use crate::{frame, TransportError, VarInt};

#[derive(Debug, Default)]
pub(super) struct Recv {
    state: RecvState,
    pub(super) assembler: Assembler,
    sent_max_stream_data: u64,
    pub(super) end: u64,
    pub(super) stopped: bool,
}

impl Recv {
    pub(super) fn new(initial_max_data: u64) -> Self {
        Self {
            state: RecvState::default(),
            assembler: Assembler::new(),
            sent_max_stream_data: initial_max_data,
            end: 0,
            stopped: false,
        }
    }

    pub(super) fn ingest(
        &mut self,
        frame: frame::Stream,
        received: u64,
        max_data: u64,
    ) -> Result<u64, TransportError> {
        let end = frame.offset + frame.data.len() as u64;
        if end >= 2u64.pow(62) {
            return Err(TransportError::FLOW_CONTROL_ERROR(
                "maximum stream offset too large",
            ));
        }

        if let Some(final_offset) = self.final_offset() {
            if end > final_offset || (frame.fin && end != final_offset) {
                debug!(end, final_offset, "final size error");
                return Err(TransportError::FINAL_SIZE_ERROR(""));
            }
        }

        let new_bytes = self.credit_consumed_by(end, received, max_data)?;

        if frame.fin {
            if self.stopped {
                // Stopped streams don't need to wait for the actual data, they just need to know
                // how much there was.
                self.state = RecvState::Closed;
            } else if let RecvState::Recv { ref mut size } = self.state {
                *size = Some(end);
            }
        }

        self.end = self.end.max(end);
        if !self.stopped {
            self.assembler.insert(frame.offset, frame.data);
        } else {
            self.assembler.set_bytes_read(end);
        }

        Ok(new_bytes)
    }

    pub(super) fn stop(&mut self) -> Result<(u64, ShouldTransmit), UnknownStream> {
        if self.stopped {
            return Err(UnknownStream { _private: () });
        }

        self.stopped = true;
        self.assembler.clear();
        // Issue flow control credit for unread data
        let read_credits = self.end - self.assembler.bytes_read();
        Ok((read_credits, ShouldTransmit(!self.is_finished())))
    }

    /// Returns the window that should be advertised in a `MAX_STREAM_DATA` frame
    ///
    /// The method returns a tuple which consists of the window that should be
    /// announced, as well as a boolean parameter which indicates if a new
    /// transmission of the value is recommended. If the boolean value is
    /// `false` the new window should only be transmitted if a previous transmission
    /// had failed.
    pub(super) fn max_stream_data(&mut self, stream_receive_window: u64) -> (u64, ShouldTransmit) {
        let max_stream_data = self.assembler.bytes_read() + stream_receive_window;

        // Only announce a window update if it's significant enough
        // to make it worthwhile sending a MAX_STREAM_DATA frame.
        // We use here a fraction of the configured stream receive window to make
        // the decision, and accomodate for streams using bigger windows requring
        // less updates. A fixed size would also work - but it would need to be
        // smaller than `stream_receive_window` in order to make sure the stream
        // does not get stuck.
        let diff = max_stream_data - self.sent_max_stream_data;
        let transmit = self.receiving_unknown_size() && diff >= (stream_receive_window / 8);
        (max_stream_data, ShouldTransmit(transmit))
    }

    /// Records that a `MAX_STREAM_DATA` announcing a certain window was sent
    ///
    /// This will suppress enqueuing further `MAX_STREAM_DATA` frames unless
    /// either the previous transmission was not acknowledged or the window
    /// further increased.
    pub fn record_sent_max_stream_data(&mut self, sent_value: u64) {
        if sent_value > self.sent_max_stream_data {
            self.sent_max_stream_data = sent_value;
        }
    }

    fn receiving_unknown_size(&self) -> bool {
        matches!(self.state, RecvState::Recv { size: None })
    }

    /// No more data expected from peer
    pub(super) fn is_finished(&self) -> bool {
        !matches!(self.state, RecvState::Recv { .. })
    }

    /// All data read by application
    pub(super) fn is_closed(&self) -> bool {
        self.state == self::RecvState::Closed
    }

    fn final_offset(&self) -> Option<u64> {
        match self.state {
            RecvState::Recv { size } => size,
            RecvState::ResetRecvd { size, .. } => Some(size),
            _ => None,
        }
    }

    /// Returns `false` iff the reset was redundant
    pub(super) fn reset(
        &mut self,
        error_code: VarInt,
        final_offset: VarInt,
        received: u64,
        max_data: u64,
    ) -> Result<bool, TransportError> {
        // Validate final_offset
        if let Some(offset) = self.final_offset() {
            if offset != final_offset.into() {
                return Err(TransportError::FINAL_SIZE_ERROR("inconsistent value"));
            }
        } else if self.end > final_offset.into() {
            return Err(TransportError::FINAL_SIZE_ERROR(
                "lower than high water mark",
            ));
        }
        self.credit_consumed_by(final_offset.into(), received, max_data)?;

        if matches!(self.state, RecvState::ResetRecvd { .. } | RecvState::Closed) {
            return Ok(false);
        }
        self.state = RecvState::ResetRecvd {
            size: final_offset.into(),
            error_code,
        };
        // Nuke buffers so that future reads fail immediately, which ensures future reads don't
        // issue flow control credit redundant to that already issued. We could instead special-case
        // reset streams during read, but it's unclear if there's any benefit to retaining data for
        // reset streams.
        self.assembler.clear();
        Ok(true)
    }

    /// Compute the amount of flow control credit consumed, or return an error if more was consumed
    /// than issued
    fn credit_consumed_by(
        &self,
        offset: u64,
        received: u64,
        max_data: u64,
    ) -> Result<u64, TransportError> {
        let prev_end = self.end;
        let new_bytes = offset.saturating_sub(prev_end);
        if offset > self.sent_max_stream_data || received + new_bytes > max_data {
            debug!(
                received,
                new_bytes,
                max_data,
                offset,
                stream_max_data = self.sent_max_stream_data,
                "flow control error"
            );
            return Err(TransportError::FLOW_CONTROL_ERROR(""));
        }

        Ok(new_bytes)
    }
}

/// Chunks
pub struct Chunks<'a> {
    id: StreamId,
    ordered: bool,
    streams: &'a mut Streams,
    pending: &'a mut Retransmits,
    state: ChunksState,
    read: u64,
}

impl<'a> Chunks<'a> {
    pub(super) fn new(
        id: StreamId,
        ordered: bool,
        streams: &'a mut Streams,
        pending: &'a mut Retransmits,
    ) -> Result<Self, ReadableError> {
        let entry = match streams.recv.entry(id) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => return Err(ReadableError::UnknownStream),
        };

        let mut recv = match entry.get().stopped {
            true => return Err(ReadableError::UnknownStream),
            false => entry.remove(),
        };

        recv.assembler.ensure_ordering(ordered)?;
        Ok(Self {
            id,
            ordered,
            streams,
            pending,
            state: ChunksState::Readable(recv),
            read: 0,
        })
    }

    /// Next
    ///
    /// Should call finalize() when done calling this.
    pub fn next(&mut self, max_length: usize) -> Result<Option<Chunk>, ReadError> {
        let mut rs = match &mut self.state {
            ChunksState::Readable(rs) => rs,
            ChunksState::Error(e, _) => return Err(e.clone()),
            ChunksState::Finished(_) => return Ok(None),
            ChunksState::Finalized => panic!("must not call next() after finalize()"),
        };

        if let Some(chunk) = rs.assembler.read(max_length, self.ordered) {
            self.read += chunk.bytes.len() as u64;
            return Ok(Some(chunk));
        }

        match rs.state {
            RecvState::ResetRecvd { error_code, .. } => {
                rs.state = RecvState::Closed;
                self.streams.stream_freed(self.id, StreamHalf::Recv);
                self.pending
                    .post_read(self.id, ShouldTransmit(false), ShouldTransmit(false), true);

                let err = ReadError::Reset(error_code);
                self.state = ChunksState::Error(err.clone(), ShouldTransmit(true));
                Err(err)
            }
            RecvState::Closed => unreachable!(),
            RecvState::Recv { size } => {
                if size == Some(rs.end) && rs.assembler.bytes_read() == rs.end {
                    rs.state = RecvState::Closed;
                    self.streams.stream_freed(self.id, StreamHalf::Recv);
                    self.pending.post_read(
                        self.id,
                        ShouldTransmit(false),
                        ShouldTransmit(false),
                        true,
                    );
                    self.state = ChunksState::Finished(ShouldTransmit(true));
                    Ok(None)
                } else {
                    let should_transmit =
                        Self::done(rs, self.read, self.id, self.streams, self.pending);
                    let old = mem::replace(
                        &mut self.state,
                        ChunksState::Error(ReadError::Blocked, should_transmit),
                    );

                    let rs = match old {
                        ChunksState::Readable(rs) => rs,
                        _ => unreachable!(),
                    };
                    self.streams.recv.insert(self.id, rs);
                    Err(ReadError::Blocked)
                }
            }
        }
    }

    /// Finalize
    pub fn finalize(mut self) -> ShouldTransmit {
        self.finalize_inner(false)
    }

    fn finalize_inner(&mut self, drop: bool) -> ShouldTransmit {
        let state = mem::replace(&mut self.state, ChunksState::Finalized);
        match state {
            ChunksState::Readable(mut rs) => {
                debug_assert!(!drop);
                let should_transmit =
                    Self::done(&mut rs, self.read, self.id, self.streams, self.pending);
                self.streams.recv.insert(self.id, rs);
                should_transmit
            }
            ChunksState::Finished(should_transmit) | ChunksState::Error(_, should_transmit) => {
                debug_assert!(!drop);
                should_transmit
            }
            ChunksState::Finalized => ShouldTransmit(false),
        }
    }

    fn done(
        rs: &mut Recv,
        read: u64,
        id: StreamId,
        streams: &mut Streams,
        pending: &mut Retransmits,
    ) -> ShouldTransmit {
        let (_, max_stream_data) = rs.max_stream_data(streams.stream_receive_window);
        let max_data = streams.add_read_credits(read);
        pending.post_read(id, max_data, max_stream_data, false);
        ShouldTransmit(max_stream_data.0 | max_data.0)
    }
}

impl<'a> Drop for Chunks<'a> {
    fn drop(&mut self) {
        let _ = self.finalize_inner(true);
    }
}

enum ChunksState {
    Readable(Recv),
    Error(ReadError, ShouldTransmit),
    Finished(ShouldTransmit),
    Finalized,
}

/// Errors triggered when reading from a recv stream
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadError {
    /// No more data is currently available on this stream.
    ///
    /// If more data on this stream is received from the peer, an `Event::StreamReadable` will be
    /// generated for this stream, indicating that retrying the read might succeed.
    #[error("blocked")]
    Blocked,
    /// The peer abandoned transmitting data on this stream.
    ///
    /// Carries an application-defined error code.
    #[error("reset by peer: code {}", 0)]
    Reset(VarInt),
    /// The stream has not been opened or was already stopped, finished, or reset
    #[error("unknown stream")]
    UnknownStream,
}

/// Errors triggered when opening a recv stream for reading
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadableError {
    /// The stream has not been opened or was already stopped, finished, or reset
    #[error("unknown stream")]
    UnknownStream,
    /// Attempted an ordered read following an unordered read
    ///
    /// Performing an unordered read allows discontinuities to arise in the receive buffer of a
    /// stream which cannot be recovered, making further ordered reads impossible.
    #[error("ordered read after unordered read")]
    IllegalOrderedRead,
}

impl From<IllegalOrderedRead> for ReadableError {
    fn from(_: IllegalOrderedRead) -> Self {
        ReadableError::IllegalOrderedRead
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum RecvState {
    Recv { size: Option<u64> },
    ResetRecvd { size: u64, error_code: VarInt },
    Closed,
}

impl Default for RecvState {
    fn default() -> Self {
        RecvState::Recv { size: None }
    }
}
