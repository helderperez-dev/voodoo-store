# Changelog

All notable changes to Voodoo Store are documented here.

## [Unreleased]

- Continued durability, quota, metrics, and binding hardening.
- Python transactions now expose atomic collection deletion; the Framework
  Runtime uses it to move fallback outbox records between native pending and
  delivered collections without splitting the commit boundary.
