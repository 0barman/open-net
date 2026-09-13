use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=DEVELOPER_DIR");
    println!("cargo:rerun-if-env-changed=SDKROOT");
    println!("cargo:rerun-if-env-changed=TOOLCHAINS");
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos")
        || std::env::var_os("DOCS_RS").is_some()
    {
        return;
    }

    // networkframework's C shim uses Clang's availability checks. Rust links
    // with -nodefaultlibs, so Clang does not add its platform-version helpers.
    // Export the native library dependency (not a final-binary link argument)
    // so applications using open-net receive these helpers as well.
    let output = Command::new("xcrun")
        .args([
            "--sdk",
            "macosx",
            "clang",
            "--print-file-name=libclang_rt.osx.a",
        ])
        .output()
        .expect("macOS network monitoring requires the Xcode command line tools");
    assert!(
        output.status.success(),
        "failed to locate Clang's macOS runtime"
    );
    let runtime = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    assert!(
        runtime.is_absolute() && runtime.is_file(),
        "Clang's macOS runtime is missing: {}",
        runtime.display()
    );
    let runtime_dir = runtime
        .parent()
        .expect("Clang runtime must have a parent directory");
    println!("cargo:rustc-link-search=native={}", runtime_dir.display());
    println!("cargo:rustc-link-lib=static=clang_rt.osx");
}
