use super::Message;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[test]
fn text_conversions_own_utf8_and_measure_bytes() -> TestResult {
    let message = {
        let borrowed = String::from("你好 Rust");
        Message::from(borrowed.as_str())
    };
    check(
        message.as_text() == Some("你好 Rust"),
        "borrowed text did not become owned",
    )?;
    check(
        message.as_bytes() == "你好 Rust".as_bytes() && message.len() == 11 && !message.is_empty(),
        "text byte view or length changed",
    )?;
    check(
        Message::text("你好 Rust") == message,
        "text constructor differs from From<&str>",
    )?;
    check(
        Message::from(String::from("你好 Rust")) == message,
        "owned text conversion differs",
    )?;
    check(
        message.into_bytes().as_ref() == "你好 Rust".as_bytes(),
        "text byte ownership conversion changed content",
    )?;
    check(
        Message::text("").is_empty() && Message::text("").len() == 0,
        "empty text must remain valid",
    )
}

#[test]
fn binary_conversions_own_borrowed_input_and_preserve_all_bytes() -> TestResult {
    let message = {
        let local = vec![0, 255, 128, 3];
        Message::from(local.as_slice())
    };
    check(
        message.as_text().is_none() && message.as_bytes() == [0, 255, 128, 3],
        "binary conversion must not interpret UTF-8",
    )?;
    check(
        message.len() == 4 && !message.is_empty(),
        "binary size changed",
    )?;
    check(
        Message::from(vec![0, 255, 128, 3]) == message,
        "owned vector conversion changed bytes",
    )?;
    check(
        Message::from(Bytes::from_static(&[0, 255, 128, 3])) == message,
        "Bytes conversion changed bytes",
    )?;
    check(
        Message::binary(vec![0, 255, 128, 3]) == message,
        "binary constructor changed bytes",
    )?;
    check(
        Message::from(&b""[..]).is_empty(),
        "empty binary input must remain valid",
    )
}

#[test]
fn binary_clones_and_into_bytes_share_the_owned_payload_until_last_drop() -> TestResult {
    /// Test owner used to verify that message cloning and body extraction do
    /// not release shared storage prematurely.
    struct Owner(
        /// Counts releases of the underlying body owner.
        Arc<AtomicUsize>,
    );
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            b"owned payload"
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let message = Message::binary(Bytes::from_owner(Owner(Arc::clone(&drops))));
    let cloned = message.clone();
    let pointer = message.as_bytes().as_ptr();
    let bytes = message.into_bytes();
    check(
        bytes.as_ptr() == pointer && cloned.as_bytes().as_ptr() == pointer,
        "Bytes ownership was copied instead of shared",
    )?;
    drop(cloned);
    check(
        drops.load(Ordering::SeqCst) == 0,
        "payload dropped before its final byte owner",
    )?;
    drop(bytes);
    check(
        drops.load(Ordering::SeqCst) == 1,
        "payload must drop exactly once",
    )
}
