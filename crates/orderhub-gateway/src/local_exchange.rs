// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under
//  the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied. See the License for the specific language governing
//  permissions and limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Minimal FIX 4.2 initiator for the local simulated exchange.
//!
//! Wire format follows the exchange's strict profile (see
//! `docs/orderhub/local-exchange.md`): NewOrderSingle requires
//! `HandlInst=1` and `TimeInForce=0` (DAY); only market and limit order
//! types are supported; cancels use 35=F with a fresh ClOrdID.
//!
//! This module is the protocol core only; wiring it as a Nautilus
//! `ExecutionClient` (with deferred event forwarding like the sandbox
//! client) is the next integration step.

use std::io::{Read, Write};
use std::net::TcpStream;

use rust_decimal::Decimal;

/// FIX field separator.
const SOH: char = '\u{1}';

/// Errors from the FIX initiator.
#[derive(Debug)]
pub enum FixError {
    /// Underlying socket I/O failure.
    Io(std::io::Error),
    /// The exchange rejected the session or a message.
    Rejected(String),
    /// A received frame was malformed.
    Malformed(String),
}

impl std::error::Error for FixError {}

impl std::fmt::Display for FixError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "FIX transport error: {err}"),
            Self::Rejected(reason) => write!(f, "FIX rejected: {reason}"),
            Self::Malformed(reason) => write!(f, "FIX malformed message: {reason}"),
        }
    }
}

impl From<std::io::Error> for FixError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// A decoded execution event for one client order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecEvent {
    /// The venue acknowledged the order (ExecType=0).
    New {
        /// Client order ID (tag 11).
        cl_ord_id: String,
        /// Venue order ID (tag 37).
        order_id: String,
    },
    /// A fill or partial fill (ExecType=1/2).
    Fill {
        cl_ord_id: String,
        order_id: String,
        /// ExecType 2 means fully filled.
        complete: bool,
        last_qty: Decimal,
        last_px: Decimal,
        cum_qty: Decimal,
    },
    /// The order was cancelled (ExecType=4).
    Canceled { cl_ord_id: String, order_id: String },
    /// The venue rejected the request (ExecType=8 or session-level 35=3).
    Rejected { cl_ord_id: String, reason: String },
    /// A session-level message with no order semantics.
    Session(String),
}

/// FIX 4.2 initiator over a TCP stream.
#[derive(Debug)]
pub struct FixInitiator {
    stream: TcpStream,
    sender_comp_id: String,
    target_comp_id: String,
    outbound_seq: u64,
    buffer: Vec<u8>,
}

impl FixInitiator {
    /// Connects and performs the FIX logon handshake.
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP connect, logon exchange, or heartbeat
    /// negotiation fails.
    pub fn connect(
        addr: &str,
        sender_comp_id: &str,
        target_comp_id: &str,
        heart_bt_int: u32,
    ) -> Result<Self, FixError> {
        let stream = TcpStream::connect(addr)?;
        let mut initiator = Self {
            stream,
            sender_comp_id: sender_comp_id.to_string(),
            target_comp_id: target_comp_id.to_string(),
            outbound_seq: 1,
            buffer: Vec::new(),
        };
        let seq = initiator.outbound_seq;
        initiator.outbound_seq += 1;
        let body = initiator.body(
            "A",
            &[
                (98, "0".to_string()),           // EncryptMethod
                (108, heart_bt_int.to_string()), // HeartBtInt
            ],
            seq,
        );
        initiator.send_raw(&frame(&body))?;

        // Await the acceptor's logon (35=A) acknowledging the session.
        loop {
            let message = initiator.read_frame()?;
            let fields = parse_fields(&message)?;
            match field(&fields, 35).ok_or_else(|| FixError::Malformed("no MsgType".into()))? {
                "A" => return Ok(initiator),
                "5" => {
                    return Err(FixError::Rejected(format!(
                        "logged out during handshake: {}",
                        field(&fields, 58).unwrap_or("no text")
                    )));
                }
                "3" => {
                    return Err(FixError::Rejected(format!(
                        "session reject: {}",
                        field(&fields, 58).unwrap_or("unspecified")
                    )));
                }
                _ => {}
            }
        }
    }

    /// Submits a new order (35=D).
    ///
    /// `price=None` sends a market order; `Some(px)` sends a limit order.
    /// TimeInForce is fixed to DAY per the exchange profile.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be sent.
    pub fn submit_order(
        &mut self,
        cl_ord_id: &str,
        symbol: &str,
        side: Side,
        quantity: Decimal,
        price: Option<Decimal>,
    ) -> Result<(), FixError> {
        let order_type = if price.is_some() { "2" } else { "1" };
        let mut fields = vec![
            (21, "1".to_string()), // HandlInst automated private execution
            (11, cl_ord_id.to_string()),
            (55, symbol.to_string()),
            (54, side.tag().to_string()),
            (60, timestamp_now()),
            (38, quantity.to_string()),
            (40, order_type.to_string()),
            (59, "0".to_string()), // TimeInForce DAY (profile-fixed)
        ];
        if let Some(px) = price {
            fields.push((44, px.to_string()));
        }
        self.send_app("D", &fields)
    }

    /// Requests cancellation of a live order (35=F).
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be sent.
    pub fn cancel_order(
        &mut self,
        cl_ord_id: &str,
        orig_cl_ord_id: &str,
        symbol: &str,
        side: Side,
    ) -> Result<(), FixError> {
        self.send_app(
            "F",
            &[
                (11, cl_ord_id.to_string()),
                (41, orig_cl_ord_id.to_string()),
                (55, symbol.to_string()),
                (54, side.tag().to_string()),
                (60, timestamp_now()),
            ],
        )
    }

    /// Reads the next frame and decodes its order semantics.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure or a malformed frame.
    pub fn next_event(&mut self) -> Result<ExecEvent, FixError> {
        let message = self.read_frame()?;
        let fields = parse_fields(&message)?;
        let msg_type =
            field(&fields, 35).ok_or_else(|| FixError::Malformed("no MsgType".into()))?;
        Ok(match msg_type {
            "8" => decode_execution_report(&fields)?,
            "3" => ExecEvent::Rejected {
                cl_ord_id: field(&fields, 11).unwrap_or_default().to_string(),
                reason: field(&fields, 58).unwrap_or("unspecified").to_string(),
            },
            "0" | "1" | "2" | "4" | "A" => ExecEvent::Session(msg_type.to_string()),
            other => ExecEvent::Session(other.to_string()),
        })
    }

    fn send_app(&mut self, msg_type: &str, fields: &[(u32, String)]) -> Result<(), FixError> {
        let seq = self.outbound_seq;
        self.outbound_seq += 1;
        let body = self.body(msg_type, fields, seq);
        self.send_raw(&frame(&body))
    }

    fn body(&self, msg_type: &str, fields: &[(u32, String)], seq: u64) -> String {
        build_body(
            msg_type,
            &self.sender_comp_id,
            &self.target_comp_id,
            seq,
            fields,
        )
    }

    fn send_raw(&mut self, message: &str) -> Result<(), FixError> {
        self.stream.write_all(message.as_bytes())?;
        self.stream.flush()?;
        Ok(())
    }

    fn read_frame(&mut self) -> Result<String, FixError> {
        read_frame(&mut self.stream, &mut self.buffer)
    }
}

/// Order side for FIX tag 54.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// Buy (1).
    Buy,
    /// Sell (2).
    Sell,
}

impl Side {
    const fn tag(self) -> u8 {
        match self {
            Self::Buy => 1,
            Self::Sell => 2,
        }
    }
}

fn decode_execution_report(fields: &[(u32, String)]) -> Result<ExecEvent, FixError> {
    let cl_ord_id = field(fields, 11).unwrap_or_default().to_string();
    let order_id = field(fields, 37).unwrap_or_default().to_string();
    let exec_type = field(fields, 150).unwrap_or("0");
    Ok(match exec_type {
        "0" => ExecEvent::New {
            cl_ord_id,
            order_id,
        },
        "1" | "2" => ExecEvent::Fill {
            cl_ord_id,
            order_id,
            complete: exec_type == "2",
            last_qty: parse_dec(field(fields, 32).unwrap_or("0"))?,
            last_px: parse_dec(field(fields, 31).unwrap_or("0"))?,
            cum_qty: parse_dec(field(fields, 14).unwrap_or("0"))?,
        },
        "4" => ExecEvent::Canceled {
            cl_ord_id,
            order_id,
        },
        "8" => ExecEvent::Rejected {
            cl_ord_id,
            reason: field(fields, 58).unwrap_or("unspecified").to_string(),
        },
        other => ExecEvent::Session(format!("ExecType {other}")),
    })
}

fn parse_dec(value: &str) -> Result<Decimal, FixError> {
    Decimal::from_str_radix_naive(value)
}

/// Minimal decimal parser (delegates to rust_decimal).
trait DecimalFromStrNaive {
    fn from_str_radix_naive(value: &str) -> Result<Decimal, FixError>;
}

impl DecimalFromStrNaive for Decimal {
    fn from_str_radix_naive(value: &str) -> Result<Decimal, FixError> {
        use rust_decimal::prelude::FromStr;
        Self::from_str(value).map_err(|_| FixError::Malformed(format!("invalid decimal {value}")))
    }
}

fn timestamp_now() -> String {
    // SendingTime (52): YYYYMMDD-HH:MM:SS in UTC.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let days = secs / 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    let rem = secs % 86_400;
    format!(
        "{year:04}{month:02}{day:02}-{:02}:{:02}:{:02}",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Civil date from days since epoch (Howard Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Builds an application-level body (tags 35/49/56/34/52 + payload).
fn build_body(
    msg_type: &str,
    sender: &str,
    target: &str,
    seq: u64,
    fields: &[(u32, String)],
) -> String {
    let mut body = String::new();
    for (tag, value) in fields {
        body.push_str(&format!("{tag}={value}{SOH}"));
    }
    format!(
        "35={msg_type}{SOH}49={sender}{SOH}56={target}{SOH}34={seq}{SOH}52={}{SOH}{}",
        timestamp_now(),
        body
    )
}

fn field(fields: &[(u32, String)], tag: u32) -> Option<&str> {
    fields
        .iter()
        .find(|(t, _)| *t == tag)
        .map(|(_, v)| v.as_str())
}

fn parse_fields(message: &str) -> Result<Vec<(u32, String)>, FixError> {
    message
        .split(SOH)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (tag, value) = part
                .split_once('=')
                .ok_or_else(|| FixError::Malformed(format!("field without '=': {part}")))?;
            let tag: u32 = tag
                .parse()
                .map_err(|_| FixError::Malformed(format!("invalid tag {tag}")))?;
            Ok((tag, value.to_string()))
        })
        .collect()
}

fn checksum(body: &str) -> u32 {
    body.bytes().map(|b| b as u32).sum::<u32>() % 256
}

fn frame(body: &str) -> String {
    let body_length = body.len();
    let head = format!("8=FIX.4.2{SOH}9={body_length}{SOH}");
    let message = format!("{head}{body}");
    format!("{message}10={:03}{SOH}", checksum(&message))
}

fn read_frame(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<String, FixError> {
    loop {
        if let Some(end) = find_frame_end(buffer) {
            let message: Vec<u8> = buffer.drain(..end).collect();
            return String::from_utf8(message)
                .map_err(|_| FixError::Malformed("non-UTF8 frame".into()));
        }
        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(FixError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "FIX connection closed",
            )));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

fn find_frame_end(buffer: &[u8]) -> Option<usize> {
    // The checksum field is the last field: "10=XXX<SOH>".
    let tail_len = "10=000\u{1}".len();
    if buffer.len() < tail_len {
        return None;
    }
    buffer
        .windows(tail_len)
        .rposition(|w| w.starts_with(b"10="))
        .map(|pos| pos + tail_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_valid_fix_utc() {
        let ts = timestamp_now();
        println!("TS={ts}");
        assert_eq!(ts.len(), 17, "got {ts}");
        assert!(ts.as_bytes()[8] == b'-', "got {ts}");
    }

    #[test]
    fn frame_roundtrip_parses_fields_and_checksum() {
        let body = format!("35=0{SOH}49=ORDERHUB{SOH}56=EXCHANGE{SOH}");
        let framed = frame(&body);
        let fields = parse_fields(&framed).unwrap();
        assert_eq!(field(&fields, 35), Some("0"));
        assert_eq!(field(&fields, 49), Some("ORDERHUB"));
        let check = field(&fields, 10).unwrap();
        let pos = framed.rfind("10=").expect("checksum field");
        assert_eq!(
            check,
            format!("{:03}", checksum(&framed[..pos])),
            "checksum covers everything before tag 10"
        );
    }

    #[test]
    fn submit_order_body_matches_exchange_profile() {
        let body = build_body(
            "D",
            "ORDERHUB",
            "EXCHANGE",
            5,
            &[
                (21, "1".into()),
                (11, "O-1".into()),
                (55, "AAPL".into()),
                (54, "1".into()),
                (38, "10".into()),
                (40, "2".into()),
                (44, "150.00".into()),
            ],
        );
        assert!(body.contains("21=1\u{1}"), "HandlInst=1 required");
        assert!(body.contains("49=ORDERHUB\u{1}"));
        assert!(body.contains("56=EXCHANGE\u{1}"));
        assert!(body.contains("34=5\u{1}"), "sequence number in body");
        assert!(body.contains("40=2\u{1}") && body.contains("44=150.00\u{1}"));
    }

    #[test]
    fn execution_report_decode_covers_lifecycle() {
        let report = format!(
            "8=FIX.4.2{SOH}9=1{SOH}35=8{SOH}37=V1{SOH}11=O-1{SOH}150=2{SOH}32=10{SOH}31=150.25{SOH}14=10{SOH}"
        );
        let fields = parse_fields(&report).unwrap();
        match decode_execution_report(&fields).unwrap() {
            ExecEvent::Fill {
                cl_ord_id,
                order_id,
                complete,
                last_qty,
                last_px,
                cum_qty,
            } => {
                assert!(complete);
                assert_eq!(cl_ord_id, "O-1");
                assert_eq!(order_id, "V1");
                assert_eq!(last_qty, Decimal::from(10));
                assert_eq!(last_px, rust_decimal::Decimal::new(15025, 2));
                assert_eq!(cum_qty, Decimal::from(10));
            }
            other => panic!("expected fill, got {other:?}"),
        }
    }
}

/// Writer half of an initiator: encodes and sends application messages.
#[derive(Debug)]
pub struct FixWriter {
    stream: TcpStream,
    sender_comp_id: String,
    target_comp_id: String,
    outbound_seq: u64,
}

impl FixWriter {
    /// Submits a new order (35=D) per the exchange profile.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be sent.
    pub fn submit_order(
        &mut self,
        cl_ord_id: &str,
        symbol: &str,
        side: Side,
        quantity: Decimal,
        price: Option<Decimal>,
    ) -> Result<(), FixError> {
        let order_type = if price.is_some() { "2" } else { "1" };
        let mut fields = vec![
            (21, "1".to_string()),
            (11, cl_ord_id.to_string()),
            (55, symbol.to_string()),
            (54, side.tag().to_string()),
            (60, timestamp_now()),
            (38, quantity.to_string()),
            (40, order_type.to_string()),
            (59, "0".to_string()),
        ];
        if let Some(px) = price {
            fields.push((44, px.to_string()));
        }
        self.send_app("D", &fields)
    }

    /// Requests cancellation (35=F).
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be sent.
    pub fn cancel_order(
        &mut self,
        cl_ord_id: &str,
        orig_cl_ord_id: &str,
        symbol: &str,
        side: Side,
    ) -> Result<(), FixError> {
        self.send_app(
            "F",
            &[
                (11, cl_ord_id.to_string()),
                (41, orig_cl_ord_id.to_string()),
                (55, symbol.to_string()),
                (54, side.tag().to_string()),
                (60, timestamp_now()),
            ],
        )
    }

    fn send_app(&mut self, msg_type: &str, fields: &[(u32, String)]) -> Result<(), FixError> {
        let seq = self.outbound_seq;
        self.outbound_seq += 1;
        let body = build_body(
            msg_type,
            &self.sender_comp_id,
            &self.target_comp_id,
            seq,
            fields,
        );
        self.stream.write_all(frame(&body).as_bytes())?;
        self.stream.flush()?;
        Ok(())
    }
}

impl FixInitiator {
    /// Splits a logged-in initiator into a writer and a background reader
    /// thread delivering decoded events until the connection closes.
    #[must_use]
    pub fn split(
        self,
    ) -> (
        FixWriter,
        std::sync::mpsc::Receiver<ExecEvent>,
        std::thread::JoinHandle<()>,
    ) {
        let Self {
            stream,
            sender_comp_id,
            target_comp_id,
            outbound_seq,
            buffer,
        } = self;
        let reader_stream = stream
            .try_clone()
            .expect("clone FIX socket for reader thread");
        let (tx, rx) = std::sync::mpsc::channel::<ExecEvent>();
        let writer = FixWriter {
            stream,
            sender_comp_id,
            target_comp_id,
            outbound_seq,
        };
        let handle = std::thread::Builder::new()
            .name("orderhub-fix-reader".to_string())
            .spawn(move || {
                let mut reader = FixReader {
                    stream: reader_stream,
                    buffer,
                };
                loop {
                    match reader.next_event() {
                        Ok(event) => {
                            if tx.send(event).is_err() {
                                return; // consumer gone
                            }
                        }
                        Err(err) => {
                            eprintln!("FIX_READER_DIED={err:?}");
                            return;
                        }
                    }
                }
            })
            .expect("spawn FIX reader thread");
        (writer, rx, handle)
    }
}

/// Blocking frame reader used by the background thread.
#[derive(Debug)]
struct FixReader {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl FixReader {
    fn next_event(&mut self) -> Result<ExecEvent, FixError> {
        let message = read_frame(&mut self.stream, &mut self.buffer)?;
        let fields = parse_fields(&message)?;
        let msg_type =
            field(&fields, 35).ok_or_else(|| FixError::Malformed("no MsgType".into()))?;
        Ok(match msg_type {
            "8" => decode_execution_report(&fields)?,
            "3" => ExecEvent::Rejected {
                cl_ord_id: field(&fields, 11).unwrap_or_default().to_string(),
                reason: field(&fields, 58).unwrap_or("unspecified").to_string(),
            },
            other => ExecEvent::Session(other.to_string()),
        })
    }
}
