# Local network test certificates

These certificates and deliberately public private keys are disposable fixtures
for loopback TLS and mTLS regression tests. Never use them outside tests.

The fixtures use ECDSA P-256 keys and SHA-256 signatures generated with OpenSSL.
`ca.pem` signs `server`, `client`, `wrong-host`, and `expired`; `other-ca.pem`
signs `other-client`. The ordinary server covers localhost, 127.0.0.1, and ::1.
The deliberately expired leaf ended in 2020; the wrong-host leaf only covers
wrong.invalid. Ordinary leaves are valid through 2040 or later. Tests do not
depend on an installed OpenSSL binary or an external network service.

CA signing private keys are not included: they are not needed to execute or
reproduce these tests. Only the leaf private keys used by test peers and client
identity validation are retained.
