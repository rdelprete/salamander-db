# Security Policy

## Supported versions

Pre-1.0: only the latest released version receives fixes.

## Reporting a vulnerability

Please report vulnerabilities privately via
[GitHub Security Advisories](https://github.com/rdelprete/salamander-db/security/advisories/new)
("Report a vulnerability" on the repository's Security tab). You should
receive an acknowledgement within a week. Please do not open public issues
for suspected vulnerabilities before a fix is available.

## Scope

SalamanderDB is an **embedded, single-process** database: it opens no
network ports and executes no remote input. The security surface that
matters here:

- **Untrusted database directories.** Opening a directory is parsing
  attacker-controllable bytes (frames, envelopes, manifests, sidecars,
  snapshots). Decoders enforce size limits before allocation and treat all
  derived state as hostile cache input — panics, unbounded allocation, or
  silent acceptance of corrupt data when opening a crafted directory are
  vulnerabilities and are in scope.
- **Durability claims.** A path by which data acknowledged at `Sync`
  durability can be lost under the documented crash model, or by which
  torn/partial writes become visible as committed events, is in scope even
  though it is not classically "security".
- **Payload contents are the application's responsibility.** The engine
  stores opaque bytes and never interprets them; unsafe deserialization of
  payloads by *application* code is out of scope.
- **Denial of service by the legitimate single writer** (e.g., appending
  until the disk fills) is out of scope.

## Integrity expectations

Checksums (CRC32C) exist to detect corruption, not to authenticate data: an
attacker with write access to the database directory can forge records.
Protect the directory with filesystem permissions; if you need tamper
evidence, layer signatures above the engine.
