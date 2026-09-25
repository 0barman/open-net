use std::error::Error;
use std::process::{Command, Output};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const ORDERS: [&str; 6] = [
    "legacy,open-net,http",
    "legacy,http,open-net",
    "open-net,legacy,http",
    "open-net,http,legacy",
    "http,legacy,open-net",
    "http,open-net,legacy",
];

fn child(provider: &str, order: &str) -> TestResult<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_tls-provider-consumer"))
        .args([provider, order])
        .env("RUST_BACKTRACE", "0")
        .output()?)
}

#[test]
fn initialized_host_supports_every_cold_start_order() -> TestResult {
    for provider in ["ring", "aws-lc"] {
        for order in ORDERS {
            let output = child(provider, order)?;
            if !output.status.success() {
                return Err(format!(
                    "provider={provider}, order={order}, status={}\nstdout:\n{}\nstderr:\n{}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                )
                .into());
            }
            let stdout = String::from_utf8(output.stdout)?;
            for step in order.split(',') {
                if !stdout.contains(&format!("completed:{step}")) {
                    return Err(format!("{provider}/{order} did not complete {step}").into());
                }
            }
            println!("passed provider={provider}, order={order}");
        }
    }
    Ok(())
}

#[test]
fn missing_default_remains_ambiguous_after_new_wss_or_http() -> TestResult {
    for order in ORDERS {
        let output = child("none", order)?;
        let stderr = String::from_utf8(output.stderr)?;
        if output.status.success()
            || !stderr
                .contains("Could not automatically determine the process-level CryptoProvider")
        {
            return Err(format!(
                "missing-provider control did not identify Rustls ambiguity: {order}\n{stderr}"
            )
            .into());
        }
        let stdout = String::from_utf8(output.stdout)?;
        for step in order.split(',').take_while(|step| *step != "legacy") {
            if !stdout.contains(&format!("completed:{step}")) {
                return Err(
                    format!("{order} failed before the expected legacy step\n{stdout}").into(),
                );
            }
        }
        println!("confirmed missing-provider ambiguity, order={order}");
    }
    Ok(())
}
