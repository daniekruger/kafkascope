# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately** — do not open a public
GitHub issue.

Use GitHub's [private vulnerability reporting][gh-report] on this repository
(Security → Report a vulnerability), or email the maintainer.

Please include:

- a description of the issue and its impact,
- steps to reproduce or a proof of concept, and
- any relevant configuration (auth mode, deployment topology).

You can expect an acknowledgement within a few days. Once a fix is available it
will be released and the report credited unless you prefer to remain anonymous.

## Scope notes

kafkascope is an operational UI over Kafka's admin and client APIs. A few things
are worth understanding when assessing risk:

- **It is only as safe as its network placement.** With `KAFKASCOPE_AUTH_MODE=none`
  (the default) there is no authentication — run it on a trusted network or put an
  auth proxy in front of it, and use `basic`/`proxy` auth for anything shared.
- **Writes require the admin role**, and `KAFKASCOPE_READONLY` (global or
  per-cluster) disables all mutations. CSRF protection guards write requests when
  authentication is enabled.
- **The broker credentials it is given define its blast radius.** Grant the
  narrowest ACLs that fit your use.

[gh-report]: https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability
