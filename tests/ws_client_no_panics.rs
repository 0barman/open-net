//! Guard the entire source directory, including inline tests and macro bodies.
//! Tokenization ignores comments and string literals without hiding nested calls.

use proc_macro2::{TokenStream, TokenTree};
use std::collections::BTreeMap;
use std::error::Error;
use std::path::{Path, PathBuf};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn forbidden_operations(stream: TokenStream, findings: &mut Vec<String>, inherited_import: bool) {
    let tokens: Vec<_> = stream.into_iter().collect();
    let mut in_import = inherited_import;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            TokenTree::Group(group) => forbidden_operations(group.stream(), findings, in_import),
            TokenTree::Punct(punct) if punct.as_char() == ';' => in_import = inherited_import,
            TokenTree::Ident(ident) => {
                let spelling = ident.to_string();
                let name = spelling.strip_prefix("r#").unwrap_or(&spelling);
                if name == "use" {
                    in_import = true;
                }
                let is_macro = matches!(
                    tokens.get(index + 1),
                    Some(TokenTree::Punct(punct)) if punct.as_char() == '!'
                );
                let is_member = index.checked_sub(1).and_then(|i| tokens.get(i)).is_some_and(
                    |previous| matches!(previous, TokenTree::Punct(p) if matches!(p.as_char(), '.' | ':')),
                );
                // Reject renamed imports at their source, while permitting the
                // std::panic module as a prefix for catch_unwind/AssertUnwindSafe.
                let is_import_leaf = in_import
                    && !matches!(tokens.get(index + 1), Some(TokenTree::Punct(p)) if p.as_char() == ':');
                let forbidden = name == "unsafe"
                    || ((is_macro || is_import_leaf)
                        && matches!(
                            name,
                            "panic"
                                | "todo"
                                | "unimplemented"
                                | "unreachable"
                                | "assert"
                                | "assert_eq"
                                | "assert_ne"
                                | "debug_assert"
                                | "debug_assert_eq"
                                | "debug_assert_ne"
                        ))
                    || ((is_member || is_import_leaf)
                        && matches!(
                            name,
                            "unwrap"
                                | "expect"
                                | "expect_err"
                                | "unwrap_err"
                                | "unwrap_unchecked"
                                | "unreachable_unchecked"
                                | "borrow"
                                | "borrow_mut"
                        ));
                if forbidden {
                    let location = ident.span().start();
                    findings.push(format!("{}:{}: {name}", location.line, location.column + 1));
                }
            }
            _ => {}
        }
    }
}

fn scan(source: &str) -> TestResult<Vec<String>> {
    let stream = source.parse::<TokenStream>()?;
    let mut findings = Vec::new();
    forbidden_operations(stream, &mut findings, false);
    Ok(findings)
}

type LegacyBaseline = BTreeMap<String, usize>;

fn remove_legacy_findings(
    relative_path: &str,
    source: &str,
    findings: Vec<String>,
    baseline: &LegacyBaseline,
) -> TestResult<Vec<String>> {
    let mut remaining = baseline.clone();
    let mut new_findings = Vec::new();
    for finding in findings {
        let key = finding_baseline_key(relative_path, source, &finding)?;
        match remaining.get_mut(&key) {
            Some(count) if *count > 0 => *count -= 1,
            _ => new_findings.push(finding),
        }
    }
    Ok(new_findings)
}

fn finding_baseline_key(relative_path: &str, source: &str, finding: &str) -> TestResult<String> {
    let (line, _) = finding.split_once(':').ok_or("invalid scanner location")?;
    let (_, operation) = finding
        .rsplit_once(": ")
        .ok_or("invalid scanner operation")?;
    let line: usize = line.parse()?;
    let index = line.checked_sub(1).ok_or("invalid scanner line")?;
    let text = source
        .lines()
        .nth(index)
        .ok_or("scanner line outside source")?;
    Ok(format!("{relative_path}\t{operation}\t{}", text.trim()))
}

fn read_legacy_baseline() -> TestResult<LegacyBaseline> {
    let mut baseline = LegacyBaseline::new();
    for line in include_str!("fixtures/network-status-no-panics-baseline.tsv").lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (count, key) = line.split_once('\t').ok_or("invalid legacy baseline row")?;
        let count: usize = count.parse()?;
        if count == 0 || baseline.insert(key.to_owned(), count).is_some() {
            return Err("invalid or duplicate legacy baseline entry".into());
        }
    }
    Ok(baseline)
}

struct ReviewedFfiAudit {
    operations: LegacyBaseline,
    fingerprints: BTreeMap<String, u64>,
}

fn is_reviewed_ffi_file(path: &str) -> bool {
    matches!(
        path,
        "src/module/net_status/inner/platform/macos.rs"
            | "src/module/net_status/inner/platform/macos_tests.rs"
    )
}

fn source_fingerprint(source: &str) -> TestResult<u64> {
    // FNV-1a is a deterministic change detector, not a security signature.
    // Token normalization ignores comments/formatting but includes FFI bodies.
    let normalized = source.parse::<TokenStream>()?.to_string();
    Ok(normalized.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    }))
}

fn parse_reviewed_ffi_audit(source: &str) -> TestResult<ReviewedFfiAudit> {
    let mut audit = ReviewedFfiAudit {
        operations: LegacyBaseline::new(),
        fingerprints: BTreeMap::new(),
    };
    for line in source.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(metadata) = line.strip_prefix("fingerprint\t") {
            let (path, value) = metadata.split_once('\t').ok_or("invalid FFI fingerprint")?;
            if !is_reviewed_ffi_file(path)
                || value.len() != 16
                || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err("invalid FFI fingerprint file or value".into());
            }
            let fingerprint = u64::from_str_radix(value, 16)?;
            if audit
                .fingerprints
                .insert(path.to_owned(), fingerprint)
                .is_some()
            {
                return Err("duplicate FFI file fingerprint".into());
            }
            continue;
        }
        let (count, key) = line.split_once('\t').ok_or("invalid FFI audit row")?;
        let count: usize = count.parse()?;
        let mut fields = key.splitn(3, '\t');
        let path = fields.next().ok_or("missing FFI audit path")?;
        let operation = fields.next().ok_or("missing FFI audit operation")?;
        let statement = fields.next().ok_or("missing FFI audit statement")?;
        if !is_reviewed_ffi_file(path)
            || operation != "unsafe"
            || statement.trim().is_empty()
            || count == 0
            || audit.operations.insert(key.to_owned(), count).is_some()
        {
            return Err("invalid or duplicate reviewed FFI allowance".into());
        }
    }
    for key in audit.operations.keys() {
        let (path, _) = key.split_once('\t').ok_or("invalid FFI allowance key")?;
        if !audit.fingerprints.contains_key(path) {
            return Err("reviewed FFI allowance has no file fingerprint".into());
        }
    }
    Ok(audit)
}

fn remove_reviewed_ffi_findings(
    relative_path: &str,
    source: &str,
    findings: Vec<String>,
    audit: &ReviewedFfiAudit,
) -> TestResult<Vec<String>> {
    if let Some(reviewed) = audit.fingerprints.get(relative_path) {
        if source_fingerprint(source)? != *reviewed {
            return Err(format!("reviewed macOS FFI source changed: {relative_path}").into());
        }
    }
    // The count-limited matcher also keys each entry by exact file, operation
    // and source line. A reviewed unsafe token cannot hide unwrap/panic/etc.
    remove_legacy_findings(relative_path, source, findings, &audit.operations)
}

#[test]
fn legacy_baseline_matches_only_the_original_operation_and_file() -> TestResult {
    let original = "let value = old.unwrap();";
    let mut baseline = LegacyBaseline::new();
    baseline.insert(format!("old.rs\tunwrap\t{original}"), 1);
    if !remove_legacy_findings("old.rs", original, scan(original)?, &baseline)?.is_empty() {
        return Err("a recorded legacy operation did not match".into());
    }
    for (path, source, expected) in [
        ("new.rs", original.to_owned(), 1),
        ("old.rs", "let value = newly_added.unwrap();".to_owned(), 1),
        ("old.rs", format!("{original}\n{original}"), 1),
        (
            "old.rs",
            "let value = old.expect(\"required\");".to_owned(),
            1,
        ),
    ] {
        if remove_legacy_findings(path, &source, scan(&source)?, &baseline)?.len() != expected {
            return Err(format!("legacy baseline hid a new operation in {path}: {source}").into());
        }
    }
    Ok(())
}

#[test]
fn reviewed_ffi_audit_matches_only_exact_statements_within_the_reviewed_count() -> TestResult {
    let path = "src/module/net_status/inner/platform/macos.rs";
    let original = "unsafe { invoke(context) };";
    let audit = test_ffi_audit(path, original, original)?;
    if !remove_reviewed_ffi_findings(path, original, scan(original)?, &audit)?.is_empty() {
        return Err("a reviewed FFI operation did not match".into());
    }
    for (file, source, expected) in [
        ("src/other.rs", original.to_owned(), 1),
        (
            "src/module/net_status/inner/platform/macos_tests.rs",
            original.to_owned(),
            1,
        ),
        (path, "unsafe { invoke(other_context) };".to_owned(), 1),
        (path, format!("{original}\n{original}"), 1),
        (path, format!("{original}\nlet value = result.unwrap();"), 1),
        (path, "unsafe { invoke(context).unwrap() };".to_owned(), 2),
    ] {
        // Keep the source fingerprint valid here to independently prove the
        // statement, operation, file and count checks cannot broaden an audit.
        let audit = test_ffi_audit(path, &source, original)?;
        if remove_reviewed_ffi_findings(file, &source, scan(&source)?, &audit)?.len() != expected {
            return Err(
                format!("FFI audit hid an unreviewed operation in {file}: {source}").into(),
            );
        }
    }
    Ok(())
}

fn test_ffi_audit(path: &str, source: &str, reviewed_line: &str) -> TestResult<ReviewedFfiAudit> {
    parse_reviewed_ffi_audit(&format!(
        "fingerprint\t{path}\t{:016x}\n1\t{path}\tunsafe\t{reviewed_line}",
        source_fingerprint(source)?
    ))
}

#[test]
fn reviewed_ffi_audit_rejects_body_changes_but_fingerprint_ignores_comments() -> TestResult {
    let path = "src/module/net_status/inner/platform/macos.rs";
    let original = "unsafe {\ninvoke(context);\n}";
    let audit = test_ffi_audit(path, original, "unsafe {")?;
    for changed in [
        "unsafe {\ninvoke(other_context);\n}",
        "unsafe {\ninvoke(context); other_operation();\n}",
        "unsafe {\ninvoke(context).unwrap();\n}",
    ] {
        if remove_reviewed_ffi_findings(path, changed, scan(changed)?, &audit).is_ok() {
            return Err("FFI body changed without invalidating the reviewed fingerprint".into());
        }
    }
    let documented = "unsafe {\n// Context remains live.\n  invoke ( context ) ;\n}";
    if !remove_reviewed_ffi_findings(path, documented, scan(documented)?, &audit)?.is_empty() {
        return Err("comments or whitespace changed the normalized FFI fingerprint".into());
    }
    Ok(())
}

#[test]
fn reviewed_ffi_fingerprint_and_line_never_exempt_panicking_operations() -> TestResult {
    let path = "src/module/net_status/inner/platform/macos.rs";
    let source = "unsafe { invoke(context).unwrap() };";
    let audit = test_ffi_audit(path, source, source)?;
    let remaining = remove_reviewed_ffi_findings(path, source, scan(source)?, &audit)?;
    if remaining.len() != 1
        || !remaining
            .iter()
            .any(|finding| finding.ends_with(": unwrap"))
    {
        return Err(
            "reviewed unsafe syntax exempted a panicking operation on the same line".into(),
        );
    }
    Ok(())
}

#[test]
fn reviewed_ffi_audit_rejects_unrelated_files_operations_and_invalid_counts() -> TestResult {
    let path = "src/module/net_status/inner/platform/macos.rs";
    let row = format!("1\t{path}\tunsafe\tunsafe {{ invoke(context) }};");
    for fixture in [
        "1\tsrc/other.rs\tunsafe\tunsafe {};".to_owned(),
        "1\tsrc/module/net_status/inner/platform/../platform/macos.rs\tunsafe\tunsafe {};"
            .to_owned(),
        format!("1\t{path}\tunwrap\tvalue.unwrap();"),
        format!("1\t{path}\tpanic\tpanic!();"),
        format!("0\t{path}\tunsafe\tunsafe {{}};"),
        format!("-1\t{path}\tunsafe\tunsafe {{}};"),
        format!("1\t{path}\tunsafe\t"),
        format!("1\t{path}\tunsafe"),
        format!("{row}\n{row}"),
        format!("fingerprint\t{path}\tnot-a-hash\n{row}"),
        format!("fingerprint\tsrc/other.rs\t0000000000000000\n{row}"),
    ] {
        let fixture = format!("fingerprint\t{path}\t0000000000000000\n{fixture}");
        if parse_reviewed_ffi_audit(&fixture).is_ok() {
            return Err(format!("FFI audit accepted an invalid allowance: {fixture}").into());
        }
    }
    if parse_reviewed_ffi_audit(&row).is_ok() {
        return Err("FFI source allowances were accepted without a file fingerprint".into());
    }
    Ok(())
}

fn rust_sources(root: &Path) -> TestResult<Vec<PathBuf>> {
    let mut directories = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                directories.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

#[test]
fn ws_client_sources_do_not_contain_panicking_operations() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/module/ws_client");
    let mut files = rust_sources(&root)?;
    if files.is_empty() {
        return Err("ws_client source scan did not inspect any Rust files".into());
    }
    // Include all new transport/configuration/session APIs and integration tests.
    // Existing unrelated API tests retain their historical baseline; new files
    // must not escape this guard merely because they live outside ws_client.
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    files.extend(rust_sources(&manifest.join("src/module/transport"))?);
    files.extend(rust_sources(&manifest.join("src/inner"))?);
    files.extend(rust_sources(&manifest.join("src/module/net_status"))?);
    // Every present and future WebSocket API participates, including inline unit tests.
    files.extend(rust_sources(&manifest.join("src/api/wsc"))?);
    files.extend(rust_sources(&manifest.join("src/api/traits/ws"))?);
    // Discover future WebSocket/network-status integration tests automatically.
    // Historical operations are matched by exact file, source line and count below;
    // a new file or an additional operation cannot inherit another file's baseline.
    files.extend(
        rust_sources(&manifest.join("tests"))?
            .into_iter()
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("ws_") || name.starts_with("net_status"))
            }),
    );
    files.extend([
        manifest.join("src/lib.rs"),
        manifest.join("src/module/mod.rs"),
        manifest.join("src/api/mod.rs"),
        manifest.join("src/api/open_net.rs"),
        manifest.join("src/api/net_error.rs"),
        manifest.join("src/api/network_config.rs"),
        manifest.join("src/api/listener.rs"),
        manifest.join("src/api/web_socket_client.rs"),
        manifest.join("src/api/wsc/mod.rs"),
        manifest.join("src/api/wsc/reconnect_policy.rs"),
        manifest.join("src/api/wsc/web_socket_connect_options.rs"),
        manifest.join("src/api/wsc/web_socket_client_config.rs"),
        manifest.join("src/api/wsc/web_socket_context_connect_options.rs"),
        manifest.join("src/api/wsc/web_socket_handshake_context.rs"),
        manifest.join("src/api/wsc/web_socket_connection_event.rs"),
        manifest.join("src/api/wsc/web_socket_connection_events.rs"),
        manifest.join("src/api/wsc/web_socket_task_event.rs"),
        manifest.join("src/api/wsc/pending_request_view.rs"),
        manifest.join("src/api/wsc/pending_request_entry.rs"),
        manifest.join("src/api/wsc/pending_request_completion.rs"),
        manifest.join("src/api/wsc/queued_request_completion.rs"),
        manifest.join("tests/ws_status_listener_integration.rs"),
        manifest.join("tests/ws_handshake_context_integration.rs"),
        manifest.join("tests/ws_network_config_integration.rs"),
        manifest.join("tests/ws_client_network_override_integration.rs"),
        manifest.join("tests/ws_public_api_compatibility.rs"),
        manifest.join("tests/ws_task_listener_integration.rs"),
        manifest.join("tests/ws_connect_shutdown_lifecycle.rs"),
        // Limit common-library coverage to this runtime ownership fix and its tests.
        manifest.join("libs/common/src/common_engine.rs"),
        manifest.join("libs/common/src/owned_runtime.rs"),
        manifest.join("libs/common/src/common_engine_runtime_tests.rs"),
        manifest.join("libs/common/src/common_engine_execution_tests.rs"),
    ]);
    files.sort();
    files.dedup();
    let baseline = read_legacy_baseline()?;
    let ffi_audit = parse_reviewed_ffi_audit(include_str!("fixtures/macos-ffi-unsafe-audit.tsv"))?;
    let mut findings = Vec::new();
    for file in files {
        let source = std::fs::read_to_string(&file)?;
        let relative = file
            .strip_prefix(manifest)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        let new_findings = remove_legacy_findings(&relative, &source, scan(&source)?, &baseline)?;
        for finding in remove_reviewed_ffi_findings(&relative, &source, new_findings, &ffi_audit)? {
            findings.push(format!("{}:{finding}", file.display()));
        }
    }
    if !findings.is_empty() {
        return Err(format!("Forbidden ws_client operations:\n{}", findings.join("\n")).into());
    }
    Ok(())
}

#[test]
fn scanner_finds_nested_macros_and_qualified_or_generic_methods() -> TestResult {
    let cases = [
        "tokio::select! { x = async { value.unwrap() } => {} }",
        "std::assert_eq!(left, right)",
        "use std::assert as ensure; ensure!(false);",
        "use std::{assert_eq as ensure_eq}; ensure_eq!(1, 2);",
        "use std::{assert_ne as ensure_ne}; ensure_ne!(1, 1);",
        "use std::panic as fail; fail!();",
        "value /* gap */ . expect(\"required\")",
        "Option::unwrap(value)",
        "value.r#unwrap()",
        "value.unwrap::<u32>()",
        "RefCell::borrow(&value)",
        "value.borrow_mut()",
        "result.expect_err(\"must fail\")",
        "result.unwrap_err()",
        "debug_assert!(valid)",
        "debug_assert_eq!(a, b)",
        "debug_assert_ne!(a, b)",
        "assert!(valid)",
        "assert_ne!(a, b)",
        "todo!()",
        "unimplemented!()",
        "unreachable!()",
        "panic!(\"failure\")",
        "value.unwrap_unchecked()",
        "std::hint::unreachable_unchecked()",
        "unsafe {}",
    ];
    for source in cases {
        if scan(source)?.len() != 1 {
            return Err(format!("scanner did not find exactly one violation in {source}").into());
        }
    }
    Ok(())
}

#[test]
fn scanner_ignores_literals_comments_and_fallible_alternatives() -> TestResult {
    let source = r###"
        // value.unwrap(); assert!(false);
        /* outer /* inner panic!() */ value.expect("x") */
        let _ = "panic!() value.unwrap()";
        let _ = r#"RefCell::borrow(&value)"#;
        let _ = value.unwrap_or(default);
        let _ = value.unwrap_or_else(|| default);
        let _ = value.unwrap_or_default();
        let _ = value.try_borrow()?;
        let _ = value.try_borrow_mut()?;
        let _ = value.ok_or_else(|| error)?;
        let _ = value.map_err(|error| error)?;
        use std::panic::{catch_unwind, AssertUnwindSafe};
    "###;
    if !scan(source)?.is_empty() {
        return Err("scanner rejected comments, literals or safe alternatives".into());
    }
    Ok(())
}
