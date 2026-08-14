# Security Policy

## Supported versions

Sierpe is pre-release; only the latest commit on `main` is supported. Once
versioned releases exist, the latest minor release will receive security
fixes.

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Use GitHub's private vulnerability reporting on this repository
(Security → Report a vulnerability), or contact the maintainer directly.

Include: affected component, reproduction steps, and impact assessment if
you have one. You will get an acknowledgement within 72 hours and a status
update within 14 days.

## Scope notes for operators

- The admin surface is gated by `ADMIN_TOKEN`; a weak token is rejected at
  boot. Never expose the admin endpoints without it.
- Sierpe never holds keys or signs transactions; it is a read-only consumer
  of public chain data. Its blast radius is the integrity of your own
  indexed data.
- Secrets are redacted from all config output; if you find a code path that
  prints one, that is a reportable vulnerability.
