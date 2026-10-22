# HTTPS proxy test certificates

These disposable ECDSA P-256 fixtures exercise a proxy trust domain independent
of the target TLS CA in `src/test_fixtures/network`. The private server key is
deliberately public and must only be used by local tests.

`ca.pem` signs `server.pem`, whose SANs cover localhost, 127.0.0.1, and ::1.
The certificates are valid from 2026-10-20 through 2046-10-15. They were generated
with OpenSSL using SHA-256, a CA with `keyCertSign,cRLSign`, and a leaf with
`digitalSignature` and `serverAuth`. The temporary CA signing key was discarded.
Tests use embedded PEM bytes and require neither OpenSSL nor external services.

The separate proxy client identity reuses `tests/fixtures/network/other-client.pem`
and `src/test_fixtures/network/other-client-key.pem`; it is trusted through
`src/test_fixtures/network/other-ca.pem`. The target uses the ordinary client
identity. This allows tests to assert the exact certificate seen by each peer.
