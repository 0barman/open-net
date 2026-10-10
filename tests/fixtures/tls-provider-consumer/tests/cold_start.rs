use std::error::Error;
use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn permutations(steps: &[&str]) -> Vec<String> {
    fn add<'a>(prefix: &mut Vec<&'a str>, remaining: Vec<&'a str>, out: &mut Vec<String>) {
        if remaining.is_empty() {
            out.push(prefix.join(","));
        }
        for index in 0..remaining.len() {
            let mut next = remaining.clone();
            prefix.push(next.remove(index));
            add(prefix, next, out);
            prefix.pop();
        }
    }
    let mut orders = Vec::new();
    add(&mut Vec::new(), steps.to_vec(), &mut orders);
    orders
}

fn child(provider: &str, order: &str) -> TestResult<Output> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tls-provider-consumer"))
        .args([provider, order])
        .env("RUST_BACKTRACE", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdout = child.stdout.take().ok_or("child stdout was not piped")?;
    let mut stderr = child.stderr.take().ok_or("child stderr was not piped")?;
    // Drain concurrently so output volume cannot bypass the parent deadline by
    // blocking a child on a full pipe. The deadline also covers runtime hangs.
    let read_stdout = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).map(|_| bytes)
    });
    let read_stderr = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).map(|_| bytes)
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            timed_out = true;
            child.kill()?;
            break child.wait()?;
        }
        thread::sleep(Duration::from_millis(10));
    };
    let stdout = read_stdout.join().map_err(|_| "stdout reader panicked")??;
    let stderr = read_stderr.join().map_err(|_| "stderr reader panicked")??;
    if timed_out {
        return Err(format!(
            "provider={provider}, order={order} exceeded 30 seconds\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr),
        )
        .into());
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn succeeds(provider: &str, order: &str) -> TestResult<String> {
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
        for marker in [
            format!("completed:{step}"),
            format!("provider-unchanged:{step}"),
        ] {
            if !stdout.lines().any(|line| line == marker) {
                return Err(format!("{provider}/{order} missing {marker}\n{stdout}").into());
            }
        }
    }
    if order.split(',').any(|step| step == "sdk-http")
        && (!stdout.contains("sdk-http-response:200:ok")
            || !stdout.contains("handshake:TLSv1_3:alpn=http/1.1"))
    {
        return Err(
            format!("{provider}/{order} lacks SDK HTTPS response/TLS evidence\n{stdout}").into(),
        );
    }
    println!("passed provider={provider}, order={order}\n{stdout}");
    Ok(stdout)
}

#[test]
fn initialized_host_supports_every_cold_start_order() -> TestResult {
    let orders = permutations(&["legacy", "open-net", "sdk-http", "http"]);
    assert_eq!(orders.len(), 24);
    for provider in ["ring", "aws-lc", "custom"] {
        for order in &orders {
            succeeds(provider, order)?;
        }
    }
    Ok(())
}

#[test]
fn no_host_provider_supports_real_sdk_http_and_ws_in_every_order() -> TestResult {
    let orders = permutations(&["open-net", "sdk-http", "http"]);
    assert_eq!(orders.len(), 6);
    for order in orders {
        succeeds("none", &order)?;
    }
    Ok(())
}

#[test]
fn missing_default_remains_ambiguous_after_sdk_or_external_http() -> TestResult {
    for order in permutations(&["legacy", "open-net", "sdk-http", "http"]) {
        let output = child("none", &order)?;
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
            if !stdout
                .lines()
                .any(|line| line == format!("completed:{step}"))
                || !stdout
                    .lines()
                    .any(|line| line == format!("provider-unchanged:{step}"))
            {
                return Err(
                    format!("{order} failed before the expected legacy step\n{stdout}").into(),
                );
            }
        }
        if stdout.lines().any(|line| line == "completed:legacy") {
            return Err("negative legacy control unexpectedly completed".into());
        }
        println!("confirmed missing-provider ambiguity, order={order}");
    }
    Ok(())
}

#[test]
fn unrestricted_providers_support_sdk_http_and_ws_tls12() -> TestResult {
    for provider in ["none", "ring", "aws-lc"] {
        for order in ["sdk-http-tls12,sdk-ws-tls12", "sdk-ws-tls12,sdk-http-tls12"] {
            let stdout = succeeds(provider, order)?;
            if !stdout.contains("sdk-http-response:200:ok")
                || !stdout.contains("handshake:TLSv1_2:alpn=http/1.1")
                || !stdout.contains("handshake:TLSv1_2:alpn=none")
            {
                return Err(format!(
                    "{provider}/{order} lacks TLS 1.2 handshake evidence\n{stdout}"
                )
                .into());
            }
        }
    }
    Ok(())
}

#[test]
fn custom_host_restricts_http_but_sdk_ws_keeps_its_local_provider() -> TestResult {
    // The TLS 1.3 positive request rules out broken roots/configuration as an
    // explanation for TLS 1.2 rejection. Both orders expose global pollution.
    for order in [
        "sdk-http,sdk-http-reject-tls12,sdk-ws-tls12",
        "sdk-ws-tls12,sdk-http-reject-tls12,sdk-http",
    ] {
        let stdout = succeeds("custom", order)?;
        if !stdout.contains("sdk-http-rejected:Tls")
            || !stdout.contains("handshake-rejected:TLSv1_2:host-restriction:")
            || !stdout.contains("handshake:TLSv1_2:alpn=none")
        {
            return Err(format!("custom/{order} lacks restriction evidence\n{stdout}").into());
        }
    }
    Ok(())
}
