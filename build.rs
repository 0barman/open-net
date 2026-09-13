#[path = "src/module/net_status/inner/platform/native/build_config.rs"]
mod macos_config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=DEVELOPER_DIR");
    println!("cargo:rerun-if-env-changed=SDKROOT");
    println!("cargo:rerun-if-env-changed=TOOLCHAINS");
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    println!("cargo:rerun-if-env-changed=MACOSX_DEPLOYMENT_TARGET");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos")
        || std::env::var_os("DOCS_RS").is_some()
    {
        return Ok(());
    }

    let native = "src/module/net_status/inner/platform/native";
    println!("cargo:rerun-if-changed={native}/path_monitor.c");
    println!("cargo:rerun-if-changed={native}/path_monitor.h");
    println!("cargo:rerun-if-changed={native}/build_config.rs");
    let requested = match std::env::var("MACOSX_DEPLOYMENT_TARGET") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    // cc otherwise derives the native deployment target from the installed
    // SDK. Use the API floor consistently across Xcode versions and honor a
    // caller's higher deployment target without changing process environment.
    let deployment = macos_config::deployment_target(
        &std::env::var("CARGO_CFG_TARGET_ARCH")?,
        requested.as_deref(),
    )?;
    cc::Build::new()
        .env("MACOSX_DEPLOYMENT_TARGET", deployment)
        .file(format!("{native}/path_monitor.c"))
        .flag("-fblocks")
        .std("c11")
        .warnings(true)
        .extra_warnings(true)
        .try_compile("open_net_path_monitor")?;
    println!("cargo:rustc-link-lib=framework=Network");
    Ok(())
}
