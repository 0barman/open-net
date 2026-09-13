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
        manifest.join("../libs/common/src/common_engine.rs"),
        manifest.join("../libs/common/src/owned_runtime.rs"),
        manifest.join("../libs/common/src/common_engine_runtime_tests.rs"),
        manifest.join("../libs/common/src/common_engine_execution_tests.rs"),
    ]);
    files.sort();
    files.dedup();
    let baseline = read_legacy_baseline()?;
    let mut findings = Vec::new();
    for file in files {
        let source = std::fs::read_to_string(&file)?;
        let relative = file
            .strip_prefix(manifest)
            .unwrap_or(&file)
            .to_string_lossy();
        for finding in remove_legacy_findings(&relative, &source, scan(&source)?, &baseline)? {
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
