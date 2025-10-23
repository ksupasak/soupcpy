# Repository Guidelines

## Project Structure & Module Organization
Two standalone Axum crates live in `agent/` and `frontend/`, each with its own `Cargo.toml`, `src/main.rs`, and shared HTML in `templates/`. Mirror protocol changes across crates unless a feature is intentionally divergent, and leave Cargo's generated `target/` output out of commits.

## Build, Test, and Development Commands
- `cargo run` (inside `agent/` or `frontend/`) starts the WebSocket demo on `http://localhost:3000`.
- `cargo check` surfaces type errors quickly; run it before committing.
- `cargo fmt` then `cargo clippy --all-targets --all-features -D warnings` keep formatting and lints clean.
- `cargo test` exercises unit and integration suites; try `cargo test websocket_loop` for a targeted run.
- `RUST_LOG=rust=debug cargo run` turns on verbose tracing via the `tracing_subscriber` filter.

## Coding Style & Naming Conventions
Use the stable toolchain with `rustfmt` defaults (4-space indentation, trailing commas as needed). Modules, functions, and variables stay in `snake_case`; types in `CamelCase`; constants in `SCREAMING_SNAKE_CASE`. Favor explicit `use` paths, keep async handlers compact with helper functions, and preserve the existing dark-theme structure when editing `templates/index.html`.

## Testing Guidelines
Add happy- and failure-path unit tests inside `#[cfg(test)]` modules near the code they cover, and create a `tests/` directory for integration flows named after the feature (`feature_name.rs`). Where WebSocket flows rely on concurrency, use Tokio’s test macros (`#[tokio::test]`) with mocked broadcast senders to stay deterministic, and add doc tests when expanding the public API.

## Commit & Pull Request Guidelines
With no commit history yet, stick to imperative subjects (`Add broadcast retry logging`) and include a short body when extra context helps. Squash WIP commits before opening a PR that explains the problem, solution, and validation (`cargo fmt`, `cargo clippy`, `cargo test`, manual QA`). Reference related issues or docs, attach console output or screenshots for UI changes, and wait for CI to pass before requesting review.

## Runtime & Configuration Tips
Favor `RUST_LOG` over ad-hoc printing; when adding async tasks, wrap them in `tracing::info_span!("ws_loop", client = %addr)` for readable logs. Keep secrets in ignored `.env` files and never commit generated TLS or credential material.
