# Retry policy

Bounded retry configuration and behavior are documented in [failure-policy.md — Retry (KL-1602)](failure-policy.md#retry-kl-1602).

Duplicate-safety requirements (the second gate before a retry sleeps) are in [idempotency.md](idempotency.md).

This file exists so spec and cross-references to `docs/retry-policy.md` resolve without duplicating content.
