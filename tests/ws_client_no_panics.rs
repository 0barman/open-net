//! Guard production source and the existing integration-test scope, including macro bodies.
//! Tokenization ignores comments and string literals without hiding nested calls.

use proc_macro2::{TokenStream, TokenTree};
use std::collections::BTreeMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

// 只识别完整绝对路径的直接函数调用；方法、别名、导入和更长路径的后缀均不豁免。
fn is_absolute_tokio_watch_borrow(tokens: &[TokenTree], index: usize) -> bool {
    const PATH: [&str; 15] = [
        ":", ":", "tokio", ":", ":", "sync", ":", ":", "watch", ":", ":", "Receiver", ":", ":",
        "borrow",
    ];
    let Some(start) = index.checked_sub(PATH.len() - 1) else {
        return false;
    };
    let Some(path) = tokens.get(start..=index) else {
        return false;
    };
    if !path
        .iter()
        .zip(PATH)
        .all(|(token, expected)| token.to_string() == expected)
        || !matches!(tokens.get(index + 1), Some(TokenTree::Group(group))
            if group.delimiter() == proc_macro2::Delimiter::Parenthesis)
    {
        return false;
    }
    // 命名空间前缀和类型参数结束符不能伪装成绝对路径的起点；此处保守限定表达式边界。
    match start
        .checked_sub(1)
        .and_then(|previous| tokens.get(previous))
    {
        None => true,
        Some(TokenTree::Ident(ident)) => {
            matches!(ident.to_string().as_str(), "return" | "break" | "yield")
        }
        Some(TokenTree::Punct(punct)) => matches!(
            punct.as_char(),
            '=' | '&' | '*' | ';' | ',' | '|' | '+' | '-' | '/' | '%' | '!'
        ),
        _ => false,
    }
}

fn is_test_only_attribute(group: &proc_macro2::Group) -> bool {
    if group.delimiter() != proc_macro2::Delimiter::Bracket {
        return false;
    }
    let mut attribute = group.stream().into_iter();
    matches!(attribute.next(), Some(TokenTree::Ident(name)) if name == "cfg")
        && matches!(attribute.next(), Some(TokenTree::Group(condition))
            if condition.delimiter() == proc_macro2::Delimiter::Parenthesis
                && condition.stream().to_string() == "test")
        && attribute.next().is_none()
}

fn test_function_or_module_follows(tokens: &[TokenTree], start: usize) -> bool {
    let mut remaining = tokens.get(start..).into_iter().flatten();
    while let Some(token) = remaining.next() {
        match token {
            TokenTree::Punct(punct) if punct.as_char() == '#' => {
                if !matches!(remaining.next(), Some(TokenTree::Group(attribute))
                    if attribute.delimiter() == proc_macro2::Delimiter::Bracket)
                {
                    return false;
                }
            }
            TokenTree::Ident(name) if name == "pub" || name == "async" || name == "const" => {}
            TokenTree::Ident(name) => return name == "fn" || name == "mod",
            _ => return false,
        }
    }
    false
}

fn forbidden_operations(stream: TokenStream, findings: &mut Vec<String>, inherited_import: bool) {
    let tokens: Vec<_> = stream.into_iter().collect();
    let mut in_import = inherited_import;
    let mut test_only_item = false;
    for (index, token) in tokens.iter().enumerate() {
        if matches!(token, TokenTree::Punct(punct) if punct.as_char() == '#')
            && matches!(tokens.get(index + 1), Some(TokenTree::Group(attribute)) if is_test_only_attribute(attribute))
            && test_function_or_module_follows(&tokens, index + 2)
        {
            test_only_item = true;
        }
        if test_only_item {
            // Only an exact outer cfg(test) proves that the item is absent from
            // production. Other cfg expressions and all production macros remain scanned.
            if matches!(token, TokenTree::Group(group) if group.delimiter() == proc_macro2::Delimiter::Brace)
                || matches!(token, TokenTree::Punct(punct) if punct.as_char() == ';')
            {
                test_only_item = false;
            }
            continue;
        }
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
                                | "resume_unwind"
                                | "borrow"
                                | "borrow_mut"
                        ));
                let safe_watch_read = name == "borrow"
                    && !in_import
                    && is_absolute_tokio_watch_borrow(&tokens, index);
                if forbidden && !safe_watch_read {
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

fn guarded_sources(manifest: &Path) -> TestResult<Vec<PathBuf>> {
    // Scan the source root so new public modules and their inline tests cannot
    // escape the guard when they move outside the legacy API directory layout.
    let mut files = rust_sources(&manifest.join("src"))?;
    if files.is_empty() {
        return Err("source scan did not inspect any Rust files".into());
    }
    // Runtime configuration and callback executors must follow the same contract
    // with every feature set. Include future sibling modules and inline tests;
    // unrelated legacy logging/platform utilities keep their existing scope.
    let common = manifest.join("libs/common/src");
    files.extend(rust_sources(&common)?.into_iter().filter(|path| {
        path.parent() == Some(common.as_path()) || path.starts_with(common.join("inner"))
    }));
    // Discover future API V2/WebSocket/network-status integration tests automatically.
    // Historical operations are matched by exact file, source line and count below;
    // a new file or an additional operation cannot inherit another file's baseline.
    files.extend(
        rust_sources(&manifest.join("tests"))?
            .into_iter()
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("api_v2_")
                            || name.starts_with("ws_")
                            || name.starts_with("net_status")
                    })
            }),
    );
    files.push(manifest.join("tests/support/session.rs"));
    files.sort();
    files.dedup();
    Ok(files)
}

fn with_source_fixture(paths: &[&str], verify: impl FnOnce(&Path) -> TestResult) -> TestResult {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    with_source_fixture_at(timestamp, paths, verify)
}

fn with_source_fixture_at(
    timestamp: u128,
    paths: &[&str],
    verify: impl FnOnce(&Path) -> TestResult,
) -> TestResult {
    static LAST_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
    let fixture_id = next_source_fixture_id(&LAST_FIXTURE_ID)?;
    let manifest = std::env::temp_dir().join(format!(
        "open-net-panic-guard-{}-{timestamp}-{fixture_id}",
        std::process::id(),
    ));
    std::fs::create_dir(&manifest)?;
    let result = (|| {
        for directory in [
            "src/module/ws_client",
            "src/module/transport",
            "src/inner",
            "src/module/net_status",
            "src/api/wsc",
            "src/api/traits/ws",
            "libs/common/src",
            "tests/support",
        ] {
            std::fs::create_dir_all(manifest.join(directory))?;
        }
        std::fs::write(manifest.join("src/module/ws_client/mod.rs"), "")?;
        for relative in paths {
            let path = manifest.join(relative);
            let parent = path.parent().ok_or("fixture file has no parent")?;
            std::fs::create_dir_all(parent)?;
            std::fs::write(path, "")?;
        }
        verify(&manifest)
    })();
    let cleanup = std::fs::remove_dir_all(&manifest);
    result?;
    cleanup?;
    Ok(())
}

fn next_source_fixture_id(last_id: &AtomicU64) -> TestResult<u64> {
    last_id
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
            last.checked_add(1)
        })
        .map(|last| last + 1)
        .map_err(|_| "source fixture identity space exhausted".into())
}

#[test]
fn source_fixture_identity_exhaustion_returns_an_error_without_reuse() -> TestResult {
    let last_id = AtomicU64::new(u64::MAX - 1);
    if next_source_fixture_id(&last_id)? != u64::MAX {
        return Err("last available source fixture identity was not allocated".into());
    }
    for _ in 0..2 {
        if next_source_fixture_id(&last_id).is_ok() || last_id.load(Ordering::Relaxed) != u64::MAX {
            return Err("exhausted source fixture identity counter wrapped or was reused".into());
        }
    }
    Ok(())
}

#[test]
fn equal_timestamp_fixtures_keep_independent_contents_and_cleanup() -> TestResult {
    let mut outer_path = None;
    let mut inner_path = None;
    with_source_fixture_at(42, &["src/outer.rs"], |outer| {
        outer_path = Some(outer.to_owned());
        std::fs::write(outer.join("src/outer.rs"), "outer contents")?;
        with_source_fixture_at(42, &["src/inner.rs"], |inner| {
            inner_path = Some(inner.to_owned());
            if outer == inner || inner.join("src/outer.rs").exists() {
                return Err("equal timestamp fixtures shared a directory".into());
            }
            if std::fs::read_to_string(outer.join("src/outer.rs"))? != "outer contents"
                || !inner.join("src/inner.rs").is_file()
            {
                return Err("nested fixture changed its parent's contents".into());
            }
            Ok(())
        })?;
        let inner = inner_path.as_ref().ok_or("inner fixture was not created")?;
        if inner.exists()
            || std::fs::read_to_string(outer.join("src/outer.rs"))? != "outer contents"
        {
            return Err("inner fixture cleanup changed the wrong directory".into());
        }
        Ok(())
    })?;
    if outer_path.ok_or("outer fixture was not created")?.exists() {
        return Err("outer fixture was not cleaned up".into());
    }
    Ok(())
}

#[test]
fn guarded_sources_cover_src_root_and_future_modules() -> TestResult {
    let included = [
        "src/lib.rs",
        "src/api/error/mod.rs",
        "src/api/subscription/mod.rs",
        "src/api/ws/mod.rs",
        "src/api/ws/request/future.rs",
        "src/api/config/defaults.rs",
        "src/api/network/proxy/future.rs",
        "src/api/net_status/snapshot.rs",
        "src/future_module/nested/implementation.rs",
    ];
    let excluded = [
        "src/api/ws/notes.txt",
        "examples/future.rs",
        "libs/common/src/unrelated/future.rs",
    ];
    let paths: Vec<_> = included.iter().chain(&excluded).copied().collect();
    with_source_fixture(&paths, |manifest| {
        let files = guarded_sources(manifest)?;
        for relative in included {
            if !files.contains(&manifest.join(relative)) {
                return Err(format!("source is absent from panic guard: {relative}").into());
            }
        }
        for relative in excluded {
            if files.contains(&manifest.join(relative)) {
                return Err(format!("source scan exceeded its boundary: {relative}").into());
            }
        }
        Ok(())
    })
}

#[test]
fn guarded_sources_cover_api_v2_tests_without_similar_names() -> TestResult {
    let included = [
        "tests/api_v2_contract.rs",
        "tests/future/api_v2_request.rs",
        "tests/ws_future.rs",
        "tests/net_status_future.rs",
        "tests/support/session.rs",
    ];
    let excluded = [
        "tests/api_v2.rs",
        "tests/api_v20_contract.rs",
        "tests/other_api_v2_contract.rs",
        "tests/api_v2_contract.txt",
        "tests/api_v2_folder/unrelated.rs",
    ];
    let paths: Vec<_> = included.iter().chain(&excluded).copied().collect();
    with_source_fixture(&paths, |manifest| {
        let files = guarded_sources(manifest)?;
        for relative in included {
            if !files.contains(&manifest.join(relative)) {
                return Err(
                    format!("integration test is absent from panic guard: {relative}").into(),
                );
            }
        }
        for relative in excluded {
            if files.contains(&manifest.join(relative)) {
                return Err(format!("test scan exceeded its boundary: {relative}").into());
            }
        }
        Ok(())
    })
}

#[test]
fn runtime_configuration_and_callback_implementation_are_guarded() -> TestResult {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = guarded_sources(manifest)?;
    for relative in [
        "src/api/open_net_config.rs",
        "src/inner/net_impl/runtime_config_tests.rs",
        "libs/common/src/lazy_callback_pool.rs",
        "libs/common/src/lazy_callback_pool_tests.rs",
        "libs/common/src/inner/common_engine_impl.rs",
        "libs/common/src/inner/common_callback_laziness_tests.rs",
    ] {
        if !files.contains(&manifest.join(relative)) {
            return Err(
                format!("runtime/callback source is absent from panic guard: {relative}").into(),
            );
        }
    }
    Ok(())
}

#[test]
fn ws_client_sources_do_not_contain_panicking_operations() -> TestResult {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = guarded_sources(manifest)?;
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
        "std::panic::resume_unwind(payload)",
        "use std::panic::resume_unwind as raise; raise(payload)",
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
// 仅放行由绝对类型路径明确指向 Tokio watch 的直接读取，保留参数内递归扫描。
fn scanner_accepts_only_absolute_tokio_watch_borrow_calls() -> TestResult {
    for source in [
        "let value = ::tokio::sync::watch::Receiver::borrow(&receiver);",
        "return ::tokio :: sync :: watch :: Receiver :: borrow(&receiver);",
        "tokio::select! { _ = ready => { ::tokio::sync::watch::Receiver::borrow(&receiver); } }",
    ] {
        let findings = scan(source)?;
        if !findings.is_empty() {
            return Err(
                format!("safe typed watch read was rejected: {source}: {findings:?}").into(),
            );
        }
    }
    for source in [
        "let value = receiver.borrow();",
        "let value = RefCell::borrow(&cell);",
        "let value = ::std::cell::RefCell::borrow(&cell);",
        "let value = tokio::sync::watch::Receiver::borrow(&receiver);",
        "let value = other::tokio::sync::watch::Receiver::borrow(&receiver);",
        "let value = ::other::tokio::sync::watch::Receiver::borrow(&receiver);",
        "let value = crate::tokio::sync::watch::Receiver::borrow(&receiver);",
        "let value = ::tokio::sync::watch::Receiver::borrow_mut(&receiver);",
        "let function = ::tokio::sync::watch::Receiver::borrow;",
        "use ::tokio::sync::watch::Receiver::borrow as renamed;",
        "::tokio::sync::watch::Receiver::borrow(&RefCell::borrow(&cell));",
        "::tokio::sync::watch::Receiver::borrow(&receiver); receiver.borrow();",
    ] {
        let findings = scan(source)?;
        if findings.len() != 1 {
            return Err(format!(
                "watch allowance changed a forbidden borrow: {source}: {findings:?}"
            )
            .into());
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

#[test]
fn scanner_excludes_only_explicit_test_items_and_keeps_production_violations() -> TestResult {
    let source = r#"
        fn production_before() { value.unwrap(); }
        #[cfg(test)]
        mod tests { fn negative_control() { value.unwrap(); assert!(false); } }
        #[cfg(test)] #[allow(dead_code)]
        fn test_helper() { value.expect("test"); }
        #[cfg(test)] mod external_tests;
        fn production_after() { value.expect("production"); }
        #[cfg(not(test))] fn release() { panic!("production"); }
        #[cfg(any(test, feature = "production"))] fn shared() { todo!(); }
        macro_rules! production_macro { () => { value.borrow_mut() }; }
        struct Mixed { #[cfg(test)] test_field: (), production: [u8; value.unwrap_err()] }
    "#;
    let findings = scan(source)?;
    for operation in [
        "unwrap",
        "expect",
        "panic",
        "todo",
        "borrow_mut",
        "unwrap_err",
    ] {
        if findings
            .iter()
            .filter(|finding| finding.ends_with(&format!(": {operation}")))
            .count()
            != 1
        {
            return Err(
                format!("production operation {operation} was hidden: {findings:?}").into(),
            );
        }
    }
    if findings.len() != 6 {
        return Err(format!("test-only code was misclassified: {findings:?}").into());
    }
    Ok(())
}
