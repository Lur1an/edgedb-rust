use std::fs;
use std::collections::HashMap;
use std::error::Error;

use bytes::{Bytes, BytesMut};

use edgedb_protocol::server_message::{ServerMessage};
use edgedb_protocol::server_message::{ServerHandshake};
use edgedb_protocol::server_message::{ErrorResponse, ErrorSeverity};
use edgedb_protocol::server_message::{ReadyForCommand, TransactionState};
use edgedb_protocol::server_message::{ServerKeyData, ParameterStatus};
use edgedb_protocol::server_message::{CommandComplete};

macro_rules! encoding_eq {
    ($message: expr, $bytes: expr) => {
        let data: &[u8] = $bytes;
        let mut bytes = BytesMut::new();
        $message.encode(&mut bytes)?;
        println!("Serialized bytes {:?}", bytes);
        let bytes = bytes.freeze();
        assert_eq!(&bytes[..], data);
        assert_eq!(ServerMessage::decode(&data.into())?, $message);
    }
}
macro_rules! map {
    ($($key:expr => $value:expr),*) => {
        {
            #[allow(unused_mut)]
            let mut h = HashMap::new();
            $(
                h.insert($key, $value);
            )*
            h
        }
    }
}


#[test]
fn server_handshake() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ServerMessage::ServerHandshake(ServerHandshake {
        major_ver: 1,
        minor_ver: 0,
        extensions: HashMap::new(),
    }), b"v\0\0\0\n\0\x01\0\0\0\0");
    Ok(())
}

#[test]
fn ready_for_command() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ServerMessage::ReadyForCommand(ReadyForCommand {
        transaction_state: TransactionState::NotInTransaction,
        headers: HashMap::new(),
    }), b"Z\0\0\0\x07\0\0I");
    Ok(())
}

#[test]
fn error_response() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ServerMessage::ErrorResponse(ErrorResponse {
        severity: ErrorSeverity::Error,
        code: 50397184,
        message: String::from("missing required connection parameter \
                               in ClientHandshake message: \"user\""),
        headers: map!{
            257 => Bytes::from_static("Traceback (most recent call last):\n  File \"edb/server/mng_port/edgecon.pyx\", line 1077, in edb.server.mng_port.edgecon.EdgeConnection.main\n    await self.auth()\n  File \"edb/server/mng_port/edgecon.pyx\", line 178, in auth\n    raise errors.BinaryProtocolError(\nedb.errors.BinaryProtocolError: missing required connection parameter in ClientHandshake message: \"user\"\n".as_bytes())
        },
    }), &fs::read("tests/error_response.bin")?[..]);
    Ok(())
}

#[test]
fn server_key_data() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ServerMessage::ServerKeyData(ServerKeyData {
        data: [0u8; 32],
    }), &fs::read("tests/server_key_data.bin")?[..]);
    Ok(())
}

#[test]
fn parameter_status() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ServerMessage::ParameterStatus(ParameterStatus {
        name: Bytes::from_static(b"pgaddr"),
        value: Bytes::from_static(b"/work/tmp/db/.s.PGSQL.60128"),
    }), &fs::read("tests/parameter_status.bin")?[..]);
    Ok(())
}

#[test]
fn command_complete() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ServerMessage::CommandComplete(CommandComplete {
        headers: HashMap::new(),
        status_data: Bytes::from_static(b"okay"),
    }), b"C\0\0\0\x0e\0\0\0\0\0\x04okay");
    Ok(())
}