# Repository Guidelines

## Project Structure & Module Organization
This workspace hosts several small Rust crates. Keep the workspace manifest at the repository root and register each crate under `crates/` in `Cargo.toml`. Create a dedicated crate such as `crates/common` for shared utilities. Binary entry points go in `src/bin/` inside each crate, while reusable logic stays in `src/lib.rs`. Store integration fixtures in `tests/` and reusable docs or diagrams in `docs/`.

```text
.
├─ Cargo.toml           # workspace manifest
├─ crates/<name>/src/   # crate source
├─ crates/<name>/tests/ # integration tests
├─ docs/                # prose and diagrams
└─ scripts/             # helper automation
```

## Build, Test, and Development Commands
- `cargo check` validates code quickly without producing artifacts; run it before pushing any change.
- `cargo fmt -- --check` ensures formatting is rustfmt-compliant; format locally with `cargo fmt`.
- `cargo clippy --all-targets --all-features -D warnings` enforces lint quality; address warnings rather than downgrading them.
- `cargo test --workspace` runs the full suite; use `cargo test -p <crate>` for targeted runs.
- `cargo run -p <crate> -- <args>` executes a binary crate locally; prefer `.env` files for configuration instead of hardcoding.

## Coding Style & Naming Conventions
Rely on the default `rustfmt` profile (stable toolchain) and 4-space indentation. Modules and functions use `snake_case`, types `CamelCase`, and constants `SCREAMING_SNAKE_CASE`. Keep files under 300 lines by factoring modules, and document public APIs with `///` comments showing the happy path and edge cases. Prefer explicit `use` paths; avoid glob imports except in test modules.

## Testing Guidelines
Unit tests live beside the code inside `#[cfg(test)]` modules; integration tests belong in `tests/` with filenames mirroring the crate or feature (`tests/auth_flow.rs`). Whenever you add a new feature, provide at least one happy-path and one failure test. If a change affects observable behavior, update or add doc tests in the corresponding `lib.rs`. Capture external dependencies with lightweight fakes to keep tests deterministic.

## Commit & Pull Request Guidelines
Write commit subjects in the imperative mood (`Add edge-case tests for parser`) and keep bodies under 72 characters per line. Squash noisy WIP commits before opening a PR. Every PR description should include a short summary, testing notes (`cargo test`, manual QA), and links to issues or design docs. Request review once CI is green, and attach screenshots or logs when changes touch CLI output.
