# Security Policy

## Supported versions

Only the latest commit on `main` is supported with security fixes.

## Reporting a vulnerability

Please do not open a public issue for security problems.
Report privately through GitHub's "Report a vulnerability" button on the
Security tab of this repository (private vulnerability reporting).

Include: a description of the issue, steps to reproduce, the affected
version or commit, and the potential impact.

You can expect an acknowledgement within 72 hours and a status update
within 7 days.

## Scope

In scope: signature verification and replay protection, dedupe handling,
admin-token authentication, secret handling, and the delivery and retry path.

Out of scope: issues that require access to your own secrets or database.

## Hardening notes for deployers

- Set `HOOKRELAY_ADMIN_TOKEN` and all provider and endpoint secrets to strong random values. Never use the docker compose defaults in production.
- Rotate provider secrets using the list form of `HOOKRELAY_PROVIDER_SECRETS` (see the README).
- Keep the admin routes off the public internet.
