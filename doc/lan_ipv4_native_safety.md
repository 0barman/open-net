# LAN IPv4 native collection safety

[LAN IPv4 guide](lan_ipv4.md) · [Library overview](../README.md)

This appendix records the native ownership and audit boundaries for maintainers.
Application developers should begin with the LAN IPv4 guide. The read-only
collectors are implemented in `src/module/net_status/lan_platform/macos.rs` and
`windows.rs`. They do not change interfaces, routes, DNS or system proxies,
register callbacks, start threads, or retain native pointers across calls.

## macOS

- The `SystemConfiguration` C ABI declarations use the framework's Core
  Foundation reference types. `SCDynamicStoreCreate` receives null callback and
  context pointers. Input strings, arrays and the store remain alive throughout
  their use.
- Every Create/Copy result is checked for null before a `core-foundation`
  create-rule RAII owner takes responsibility for releasing it, including early
  returns. `SCError` describes only the most recent native error.
- The array returned by `SCNetworkInterfaceCopyAll` contains `CFType` elements.
  Each element's type is checked against `SCNetworkInterfaceGetTypeID` before
  interface-specific getters are called. The array owns its elements. Strings
  from GetBSDName/GetInterfaceType are checked for null and then retained using
  the get rule.
- Dynamic-store dictionaries come from system APIs; dictionary lookup does not
  accept externally supplied pointers. Values are checked for null, retained
  with the get rule, and checked for the expected dictionary, string or boolean
  type. An unexpected type is not treated as evidence about a link or route.
- Interface-type collection stops with a diagnostic if its array exceeds 65,536
  entries. Dictionary lookups are limited to global IPv4 and interface Link
  keys. Returned data is never executed.

A missing `PrimaryInterface` remains unknown. Failure to read dynamic-store
metadata does not erase an already collected interface type. Tests use
process-created CF dictionaries for active Wi-Fi, an inactive bridge, missing
fields and unexpected field types. They do not simulate the operating system
violating its memory-allocation contract.

## Windows

- The collector uses the IP Helper bindings from the `windows` crate. Arguments to
  `GetIfEntry2` and `GetIpInterfaceEntry` are fully initialized, correctly aligned
  local structures. Interface indices and address families are populated before
  writable pointers are passed to the APIs.
- `GetIpForwardTable2` receives a valid output pointer. Its result is checked for
  null even after success, then immediately placed under a local RAII owner.
  `FreeMibTable` runs exactly once on both normal and early-return paths.
- `NumEntries` comes from the successfully returned native table header. Counts
  above 65,536 are rejected before reading entries. The trailing slice follows
  the native allocation length, not the Rust declaration's single-element array
  placeholder. It is used only while the table owner remains alive. Safety
  relies on Windows honoring the documented layout and allocation contract.
- Route and interface metrics are added as `u64` to avoid overflowing two `u32`
  values. Each interface metric is queried once by index. If any required
  metric cannot be obtained, the default-route role remains unknown; a partial
  ranking is not presented as complete.
- Physical type, link state and default-route role are independent evidence.
  Native Unknown states remain unknown. A VPN may retain its actual default-route
  role; the recommendation policy separately decides whether to exclude it.

Pure conversion tests cover physical/hosted Wi-Fi, unknown/disconnected links,
and unknown, empty or tied default-route results. Cross-compilation can check
Rust types and ABI binding usage; it cannot replace native Windows execution or
LAN connectivity testing.

## Source guard

`tests/ws_client_no_panics.rs` scans the production and network-status
sources. Its reviewed FFI file set includes the two collector paths
above. `tests/fixtures/lan-ffi-unsafe-audit.tsv` records each exact unsafe
statement, its count and a normalized token fingerprint of its complete file.
Adding or changing an operation, duplicating one, or adding panic/unwrap on the
same line causes a mismatch with the recorded audit fingerprints.

`network-status-no-panics-baseline.tsv` records the broader source-guard baseline.
Changes to FFI implementation or audited tokens require source review before
updating the corresponding fingerprint. The fingerprint detects changes; it is
not a safety proof or cryptographic signature.
