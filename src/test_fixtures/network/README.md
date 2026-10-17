# Shared network test certificates

These certificates and deliberately public private keys are disposable fixtures
for library unit tests and repository integration tests. Never use them outside
tests. They are referenced only by test code and are not embedded in production
builds.

The fixtures use ECDSA P-256 keys and SHA-256 signatures. `ca.pem` signs the
ordinary client identity; `other-ca.pem` and `other-client-key.pem` exercise
alternative trust roots and mismatched identities. CA signing private keys are
not included.

These shared fixtures live under `src` so `cargo test --lib --all-features` can
run from an extracted release crate without the repository's `tests` directory.
Additional loopback server certificates and integration-only fixtures remain in
`tests/fixtures/network` in the repository.
