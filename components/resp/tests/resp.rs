//! Tests for the `resp` component, which converts between its exported types and the codec's.

use test_harness::Harness;
use test_harness::resp_bindings::exports::componentized::valkey::resp::Value;

/// One value of every type, nested values are encoded RESP.
fn values() -> Vec<Value> {
    vec![
        Value::String("OK".to_string()),
        Value::Error("ERR oops".to_string()),
        Value::Integer(-42),
        Value::BulkString("hello\r\nworld".to_string()),
        Value::Array(vec![b"+a\r\n".to_vec(), b":1\r\n".to_vec()]),
        Value::Null,
        Value::Boolean(true),
        Value::Double(1.5),
        Value::BigNumber("3492890328409238509324850943850943825024385".to_string()),
        Value::BulkError("SYNTAX invalid".to_string()),
        Value::VerbatimString(("txt".to_string(), "Some string".to_string())),
        Value::Map(vec![(b"+key\r\n".to_vec(), b"+value\r\n".to_vec())]),
        Value::Set(vec![b"+a\r\n".to_vec(), b"+b\r\n".to_vec()]),
        Value::Push(vec![b"+message\r\n".to_vec()]),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn values_round_trip() -> wasmtime::Result<()> {
    let mut harness = Harness::new("resp").await?;
    harness
        .run(async |accessor, exports| {
            for value in values() {
                let encoded = exports
                    .resp()
                    .call_encode(accessor, value.clone())
                    .await?
                    .expect("encode");
                let decoded = exports
                    .resp()
                    .call_decode(accessor, encoded)
                    .await?
                    .expect("decode");
                assert_eq!(decoded, value);
            }
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn encodes_resp() -> wasmtime::Result<()> {
    let mut harness = Harness::new("resp").await?;
    harness
        .run(async |accessor, exports| {
            let encoded = exports
                .resp()
                .call_encode(
                    accessor,
                    Value::Array(vec![b"$3\r\nGET\r\n".to_vec(), b"$1\r\na\r\n".to_vec()]),
                )
                .await?;
            assert_eq!(encoded, Ok(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n".to_vec()));
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_resp_fails_to_decode() -> wasmtime::Result<()> {
    let mut harness = Harness::new("resp").await?;
    harness
        .run(async |accessor, exports| {
            let decoded = exports
                .resp()
                .call_decode(accessor, b"?nope\r\n".to_vec())
                .await?;
            assert!(decoded.is_err(), "decoded {decoded:?}");
            Ok(())
        })
        .await
}
