use std::io;

#[path = "../src/module/net_status/inner/platform/native/build_config.rs"]
mod build_config;

fn check_target(arch: &str, requested: Option<&str>, expected: &str) -> io::Result<()> {
    let actual = build_config::deployment_target(arch, requested)?;
    if actual != expected {
        return Err(io::Error::other(format!(
            "{arch} target {requested:?}: expected {expected}, received {actual}"
        )));
    }
    Ok(())
}

fn check_rejected(arch: &str, requested: Option<&str>) -> io::Result<()> {
    match build_config::deployment_target(arch, requested) {
        Ok(actual) => Err(io::Error::other(format!(
            "{arch} target {requested:?} unexpectedly accepted as {actual}"
        ))),
        Err(error) if error.to_string().is_empty() => Err(io::Error::other(
            "deployment target error has no diagnostic",
        )),
        Err(_) => Ok(()),
    }
}

#[test]
fn absent_target_uses_architecture_minimum_instead_of_sdk_version() -> io::Result<()> {
    check_target("x86_64", None, "10.14")?;
    check_target("aarch64", None, "11.0")
}

#[test]
fn explicit_target_at_architecture_minimum_is_preserved() -> io::Result<()> {
    for target in ["10.14", "10.14.0", "010.014.000"] {
        check_target("x86_64", Some(target), target)?;
    }
    for target in ["11", "11.0", "11.0.0", "011.000.000"] {
        check_target("aarch64", Some(target), target)?;
    }
    Ok(())
}

#[test]
fn newer_target_with_one_two_or_three_components_is_preserved() -> io::Result<()> {
    for target in ["10.14.1", "10.15", "10.99.123"] {
        check_target("x86_64", Some(target), target)?;
    }
    for arch in ["x86_64", "aarch64"] {
        for target in ["11.0.1", "11.1", "15", "15.2", "26.0.1"] {
            check_target(arch, Some(target), target)?;
        }
    }
    Ok(())
}

#[test]
fn target_below_architecture_minimum_is_rejected() -> io::Result<()> {
    for target in ["0", "9.99.99", "10", "10.13", "10.13.99"] {
        check_rejected("x86_64", Some(target))?;
    }
    for target in ["0", "10", "10.14", "10.16.9", "10.99.99"] {
        check_rejected("aarch64", Some(target))?;
    }
    Ok(())
}

#[test]
fn malformed_target_is_rejected_without_panicking() -> io::Result<()> {
    for arch in ["x86_64", "aarch64"] {
        for target in [
            "",
            " 11.0",
            "11.0 ",
            "+11.0",
            "-11.0",
            "11.",
            ".11",
            "11..0",
            "11.0.0.1",
            "11.0-beta",
            "11\n",
            "１１.０",
            "十一",
            "11.1e1",
            "4294967296",
            "11.4294967296",
            "11.0.4294967296",
        ] {
            check_rejected(arch, Some(target))?;
        }
    }
    Ok(())
}

#[test]
fn unsupported_architecture_is_rejected_with_or_without_explicit_target() -> io::Result<()> {
    for arch in ["", "arm64", "i686", "arm", "riscv64", "X86_64"] {
        check_rejected(arch, None)?;
        check_rejected(arch, Some("15.0"))?;
    }
    Ok(())
}
