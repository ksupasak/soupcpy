# Platform Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn two standalone Axum crates into a Cargo workspace with a Postgres-backed `core` domain crate, and ship a themed admin shell with user login, cookie sessions and profile editing.

**Architecture:** A new `core` crate owns the domain (users, sessions, passwords, migrations) and must not depend on `axum`; it returns domain types and `core::Error`. The gateway keeps its existing WebSocket relay untouched — moved verbatim into `src/relay/` — and gains a server-rendered portal in `src/web/` built on minijinja, Tailwind and HTMX. The agent is moved into the workspace and otherwise left alone.

**Tech Stack:** Rust stable (edition 2021), axum 0.7, sqlx with the `postgres` driver, Postgres 16, argon2, minijinja, HTMX, Tailwind standalone CLI.

**Spec:** `docs/superpowers/specs/2026-10-20-soupcpy-platform-foundation-design.md`

## Global Constraints

- `core` must not depend on `axum`, or on any HTTP type. Violating this fails review regardless of tests.
- Every file stays under 300 lines (`AGENTS.md`). Split by responsibility when a file approaches it.
- Before every commit: `cargo fmt`, then `cargo clippy --all-targets --all-features -- -D warnings`, then `cargo test --workspace`. All three clean.
- Shared dependency versions are declared once in root `[workspace.dependencies]`; member crates use `workspace = true`.
- All timestamps are `timestamptz`, stored UTC.
- Money, when it arrives in a later slice, is `numeric(14,2)`. Never floats, never hand-rolled cents.
- `role` is one of `user`, `operator`, `admin`. `status` is one of `active`, `suspended`. Both CHECK-constrained text, not Postgres enums.
- Uniqueness: `lower(username)`; `lower(email)` where email is not null; `phone` where phone is not null.
- Passwords are argon2id. Session tokens are 256-bit random, stored only as SHA-256.
- Session cookie: `HttpOnly`, `SameSite=Lax`, `Secure` (configurable off for local HTTP). Sliding `last_seen_at` under a 30-day absolute cap.
- No credential, token, session value or password hash is logged at any level. `af8ebab` was a log-leakage fix in this codebase.
- New configuration follows the existing `ServerConfig::load_file()` + `apply_env_overrides()` pattern with a `GATEWAY_*` name, not a second mechanism.
- Commit subjects are imperative; body lines wrap at 72 columns. End commit messages with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.

**Prerequisites before Task 1** (both are open questions in the spec, §13):

1. This checkout is shared with another worker, currently on `fix/gateway-websocket-session-routing` with uncommitted agent edits. Task 1 moves every path in the repo and **cannot** run here safely. Either land their branch first, or run this plan in a dedicated worktree created via `superpowers:using-git-worktrees`.
2. `docker network prune` must succeed first. `docker-compose.yml` is pinned to `network_mode: bridge` because this host "has exhausted its network address pools (33 networks)"; Postgres is the second service that comment warns about, and it will not resolve by name until that is cleared.

## Review Focus

Five failure modes the spec implies but which no task's happy path exercises, most likely to bite first. Each has a test placed in the task that owns the code.

1. **A forged or garbage session cookie** — attacker-controlled bytes reach `hash_token`. Expected: redirect to `/login`, never a panic from a decode. (Task 9)
2. **User enumeration through login timing** — argon2 runs only when the username exists, so response time reveals which accounts are real. Expected: a dummy verify on the unknown-user path so both cost the same. (Task 8)
3. **Oversized input** — a 1 MB password or a 10,000-character display name. Expected: a validation error; argon2 must never be handed an unbounded string. (Task 4)
4. **Two concurrent creates of the same username** — the unique index, not the application check, decides. Expected: `Error::Conflict`, never a 500 leaking a constraint name. (Task 4)
5. **A malformed email or phone accepted silently** — phase 3 then cannot send an OTP to it. Expected: rejected at the service boundary. (Task 4)

---

### Task 1: Workspace skeleton and crate moves

Pure relocation. No behaviour changes, no new dependencies. The 16 existing gateway tests are the proof.

**Files:**
- Create: `Cargo.toml` (workspace manifest)
- Move: `soupcpy-agent/` → `crates/agent/`, `soupcpy-gateway/` → `crates/gateway/` (use `git mv`)
- Modify: `crates/gateway/Cargo.toml` (rename package `rust` → `soupcpy-gateway`)
- Modify: `crates/gateway/Dockerfile` (the `COPY --from=builder /build/target/release/rust` line and its comment; build context is now the workspace root)
- Modify: `crates/gateway/docker-compose.yml` (build context)
- Modify: `crates/agent/start.sh` (paths)
- Modify: `.gitignore` (single root `target/`)

**Interfaces:**
- Consumes: nothing.
- Produces: the workspace. Every later task adds members to `[workspace.members]` and shared versions to `[workspace.dependencies]`.

- [ ] **Step 1: Record the pre-move baseline**

Run: `cd soupcpy-gateway && cargo test 2>&1 | tail -3`
Expected: 16 tests pass. Note the exact count; Step 5 must match it.

- [ ] **Step 2: Move both crates and write the workspace manifest**

`git mv` both directories. Root `Cargo.toml` declares `[workspace]` with `members = ["crates/agent", "crates/gateway"]`, `resolver = "2"`, and a `[workspace.dependencies]` block holding the union of both crates' current dependencies at their existing versions. Unify `sqlx` on one version carrying both the `sqlite` and `postgres` drivers rather than compiling two copies. Member manifests switch to `workspace = true`.

- [ ] **Step 3: Rename the gateway package and fix the Dockerfile**

Package becomes `soupcpy-gateway`, so the built binary is `soupcpy-gateway`. Delete the Dockerfile comment explaining the old `rust` binary name — it documents a fact that no longer exists.

- [ ] **Step 4: Verify the moved tests still pass**

Run: `cargo test --workspace 2>&1 | tail -5`
Expected: the same 16 tests pass, 0 failures.

- [ ] **Step 5: Verify the container still builds and serves**

Run: `docker compose -f crates/gateway/docker-compose.yml build`
Expected: build succeeds and `COPY` finds `soupcpy-gateway`.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "Move both crates into a Cargo workspace"
```

---

### Task 2: core crate, Postgres and the test harness

**Files:**
- Create: `crates/core/Cargo.toml`, `crates/core/src/lib.rs`, `crates/core/src/db.rs`, `crates/core/src/error.rs`
- Create: `crates/core/migrations/0001_users_and_sessions.sql`
- Create: `.env.example`
- Modify: `Cargo.toml` (add the member), `.gitignore` (add `.env`)
- Modify: `crates/gateway/docker-compose.yml` (add a `postgres:16` service with a named volume)
- Test: `crates/core/tests/migrations.rs`

**Interfaces:**
- Produces:
  - `core::Error` — enum with `NotFound`, `Conflict(String)`, `Unauthorized`, `Validation(Vec<FieldError>)`, `Db(sqlx::Error)`; `core::FieldError { field: String, message: String }`; `core::Result<T>`.
  - `core::db::connect(database_url: &str, max_connections: u32) -> core::Result<PgPool>`
  - `core::db::migrate(pool: &PgPool) -> core::Result<()>` wrapping `sqlx::migrate!()`.

The migration is the DDL in spec §5, verbatim, including all three partial unique indexes.

- [ ] **Step 1: Write the failing test**

```rust
#[sqlx::test(migrations = "./migrations")]
async fn migrations_apply_and_constrain(pool: PgPool) {
    // users accepts a row with null email and null phone
    // a second row whose username differs only in case is rejected
    // role = 'owner' is rejected by the check constraint
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p soupcpy-core --test migrations`
Expected: FAIL — the crate or migration does not exist yet.

- [ ] **Step 3: Bring up Postgres and point the tests at it**

Add the compose service and a named volume. `.env.example` documents `DATABASE_URL`; `.env` is gitignored. `#[sqlx::test]` reads `DATABASE_URL` and creates a throwaway database per test, so no test shares state.

- [ ] **Step 4: Write the migration, `error.rs` and `db.rs`**

`Error` derives `Debug` and implements `Display`; `From<sqlx::Error>` maps a unique-violation (SQLSTATE `23505`) to `Conflict` carrying the constraint name, everything else to `Db`. That mapping is what makes Review Focus item 4 work.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p soupcpy-core --test migrations`
Expected: PASS, 3 assertions.

- [ ] **Step 6: Commit**

```bash
git add crates/core Cargo.toml .env.example .gitignore crates/gateway/docker-compose.yml
git commit -m "Add core crate with Postgres schema and test harness"
```

---

### Task 3: Password hashing

**Files:**
- Create: `crates/core/src/auth/mod.rs`, `crates/core/src/auth/password.rs`

**Interfaces:**
- Produces:
  - `core::auth::password::MAX_PASSWORD_LEN: usize = 1024`
  - `hash_password(plain: &str) -> core::Result<String>` — argon2id, parameters pinned in a constant with a comment recording the tuning target (~100 ms on the deployment host)
  - `verify_password(plain: &str, hash: &str) -> bool`
  - `dummy_verify(plain: &str)` — verifies against a baked-in hash and discards the result, so the unknown-user login path costs the same as a real one

- [ ] **Step 1: Write the failing tests**

```rust
fn hash_then_verify_accepts_the_same_password()
fn verify_rejects_a_wrong_password()
fn two_hashes_of_one_password_differ()          // salted
fn hash_password_rejects_input_over_max_len()   // Err(Validation), not a panic
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-core auth::password`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `password.rs`**

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-core auth::password`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add argon2id password hashing"
```

---

### Task 4: User repository and service

Owns Review Focus items 3, 4 and 5.

**Files:**
- Create: `crates/core/src/user/mod.rs` (types), `crates/core/src/user/repo.rs` (SQL), `crates/core/src/user/service.rs` (rules)

**Interfaces:**
- Consumes: `core::Error`, `core::db`, `core::auth::password`.
- Produces:
  - `core::user::{User, Role, Status, NewUser, ProfileUpdate}`. `Role` and `Status` are enums with `as_str`/`FromStr`. `User` has `id: i64`, `username: String`, `email: Option<String>`, `phone: Option<String>`, `display_name: String`, `role`, `status`, `email_verified_at`, `phone_verified_at`, `created_at`, `updated_at`.
  - `repo::insert(pool, &NewUser, password_hash: &str) -> Result<User>`
  - `repo::find_by_id(pool, id: i64) -> Result<User>`
  - `repo::find_credentials_by_username(pool, username: &str) -> Result<Option<(User, String)>>` — the `String` is the password hash
  - `repo::update_profile(pool, id: i64, &ProfileUpdate) -> Result<User>`
  - `repo::update_password_hash(pool, id: i64, hash: &str) -> Result<()>`
  - `repo::set_status`, `repo::set_role`, `repo::list(pool, limit: i64, offset: i64) -> Result<Vec<User>>`
  - `service::create_user(pool, NewUser) -> Result<User>`
  - `service::update_profile(pool, id: i64, ProfileUpdate) -> Result<User>`
  - `service::change_password(pool, id: i64, current: &str, new: &str) -> Result<()>`
  - `service::authenticate(pool, username: &str, password: &str) -> Result<User>` — returns `Error::Unauthorized` for an unknown user, a wrong password, or a non-active status; calls `password::dummy_verify` on the unknown-user path; and rehashes and stores the password when its hash carries weaker parameters than the current ones (spec §6.3)
  - Limits: `MAX_USERNAME_LEN = 64`, `MAX_DISPLAY_NAME_LEN = 128`

- [ ] **Step 1: Write the failing tests**

```rust
#[sqlx::test] async fn create_then_find_round_trips()
#[sqlx::test] async fn create_allows_null_email_and_phone()
#[sqlx::test] async fn duplicate_username_differing_only_in_case_is_conflict()   // Review Focus 4
#[sqlx::test] async fn duplicate_email_differing_only_in_case_is_conflict()
#[sqlx::test] async fn update_profile_changing_email_clears_email_verified_at()
#[sqlx::test] async fn update_profile_changing_phone_clears_phone_verified_at()
#[sqlx::test] async fn change_password_requires_the_current_password()
         fn display_name_over_max_len_is_validation_error()                      // Review Focus 3
         fn password_over_max_len_is_validation_error()                          // Review Focus 3
         fn malformed_email_is_validation_error()                                // Review Focus 5
         fn phone_not_in_e164_is_validation_error()                              // Review Focus 5
#[sqlx::test] async fn authenticate_accepts_valid_credentials()
#[sqlx::test] async fn authenticate_rejects_a_suspended_user_as_unauthorized()
#[sqlx::test] async fn authenticate_rehashes_a_hash_with_weaker_parameters()
```

`update_profile` clearing `verified_at` is the spec §6.9 rule; assert it only clears the field that actually changed.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-core user::`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the three modules**

Validation runs in `service`, not `repo`: `repo` is SQL only. E.164 is a leading `+` followed by 8–15 digits.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-core user::`
Expected: PASS, 14 tests.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add user repository and service with validation"
```

---

### Task 5: Session store

**Files:**
- Create: `crates/core/src/auth/session.rs`

**Interfaces:**
- Produces:
  - `ABSOLUTE_MAX_AGE: Duration` = 30 days
  - `generate_token() -> String` — 256 bits from the OS RNG, URL-safe base64
  - `hash_token(token: &str) -> [u8; 32]` — SHA-256; must accept arbitrary bytes without panicking
  - `create(pool, user_id: i64, ip: Option<IpAddr>, user_agent: Option<&str>) -> Result<String>` returns the raw token, which is never stored
  - `lookup(pool, token: &str) -> Result<Option<User>>` — refreshes `last_seen_at`, rejects expired rows, and **rejects a user whose status is not `active`**, deleting that session
  - `delete(pool, token: &str) -> Result<()>`
  - `delete_expired(pool) -> Result<u64>`

- [ ] **Step 1: Write the failing tests**

```rust
#[sqlx::test] async fn create_then_lookup_returns_the_user()
#[sqlx::test] async fn the_raw_token_is_not_stored_anywhere()      // assert no row's token_hash equals the token bytes
#[sqlx::test] async fn lookup_rejects_an_expired_session()
#[sqlx::test] async fn lookup_of_a_suspended_user_deletes_the_session()
#[sqlx::test] async fn lookup_refreshes_last_seen_at()
#[sqlx::test] async fn delete_expired_removes_only_expired_rows()
         fn two_generated_tokens_differ()
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-core auth::session`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `session.rs`**

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-core auth::session`
Expected: PASS, 7 tests.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add server-side session store with hashed tokens"
```

---

### Task 6: Admin bootstrap binary

**Files:**
- Create: `crates/core/src/bin/seed_admin.rs`
- Test: `crates/core/tests/seed_admin.rs`

**Interfaces:**
- Consumes: `user::service::create_user`, `repo::update_password_hash`, `repo::set_role`.
- Produces: a `seed-admin` binary, and `seed_admin::run(pool, username: &str, password: &str) -> core::Result<User>` so the test can call it without spawning a process.

Reads `SEED_ADMIN_USERNAME` and `SEED_ADMIN_PASSWORD` from the environment only — never a CLI argument, so the password stays out of shell history and the process list.

- [ ] **Step 1: Write the failing tests**

```rust
#[sqlx::test] async fn run_creates_an_admin_when_none_exists()
#[sqlx::test] async fn run_twice_updates_the_password_instead_of_failing()   // idempotent
#[sqlx::test] async fn run_promotes_an_existing_non_admin_user()
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-core --test seed_admin`
Expected: FAIL.

- [ ] **Step 3: Implement `run` and the thin `main` that reads the two env vars**

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-core --test seed_admin`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add idempotent seed-admin bootstrap binary"
```

---

### Task 7: Gateway web scaffold

Moves the relay and mounts an empty themed portal beside it. This is the strangler boundary: relay routes must behave identically before and after.

**Files:**
- Move: relay code out of `crates/gateway/src/main.rs` into `crates/gateway/src/relay/mod.rs` — `AppState`, `ClientHandle`, `ConnectionRegistry`, `ChannelStore`, `ChannelDescriptor`, `EndpointKind`, `AppSettings`, `PublicScheme`, and the handlers behind `/ws*`, `/api/*`, `/`, `/multiview`, `/stream`, `/health`. Verbatim; no logic edits.
- Create: `crates/gateway/src/web/{mod,error,templates}.rs`
- Create: `crates/gateway/templates/{layout,login,profile,error}.html`, `crates/gateway/templates/admin/users.html`
- Create: `scripts/build-css.sh`, `crates/gateway/assets/app.css` (built output, committed)
- Modify: `crates/gateway/src/main.rs` — thin: config, state, router assembly, serve. `ServerConfig` gains `database_url`, with a `GATEWAY_DATABASE_URL` override added to `apply_env_overrides`.

**Interfaces:**
- Consumes: `core::db::{connect, migrate}`.
- Produces:
  - `web::WebState { pool: PgPool, templates: minijinja::Environment<'static>, cookie_secure: bool }`
  - `web::router(state: WebState) -> axum::Router`
  - `web::error::WebError` — wraps `core::Error`, implements `IntoResponse`, rendering the themed error page for a document request and a partial for an `HX-Request`.
  - `layout.html` renders flash messages from a one-shot cookie, so a redirect after a successful POST can carry a confirmation (spec §8).
  - A background task calling `session::delete_expired` hourly, wrapped in `tracing::info_span!("session_sweep")` per the `AGENTS.md` runtime guidance (spec §6.4).

Templates load from disk in debug and are embedded in release. Tailwind is the standalone CLI via `scripts/build-css.sh`; CI reruns it and fails on a diff.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn get_login_renders_a_form()                    // 200, contains name="username"
#[tokio::test] async fn an_unknown_path_renders_the_themed_404()
#[tokio::test] async fn health_still_returns_ok_after_the_relay_move()
```

Router tests use `tower::ServiceExt::oneshot`.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-gateway web::`
Expected: FAIL.

- [ ] **Step 3: Move the relay into `src/relay/`, unchanged**

- [ ] **Step 4: Verify the move changed nothing**

Run: `cargo test -p soupcpy-gateway`
Expected: the 16 pre-existing tests still pass.

- [ ] **Step 5: Add the web module, templates, CSS build and migrate-on-boot**

Migrations run at startup via `core::db::migrate`, logged, and the process exits non-zero if they fail.

- [ ] **Step 6: Run to verify the new tests pass**

Run: `cargo test -p soupcpy-gateway`
Expected: PASS, 19 tests.

- [ ] **Step 7: Manually confirm the relay is intact**

Run the gateway, open `/multiview`, confirm a live scrcpy stream still plays. The spec's definition of done requires this verified by hand.

- [ ] **Step 8: Commit**

```bash
git commit -am "Split gateway into relay and web modules"
```

---

### Task 8: Login, logout and CSRF

Owns Review Focus item 2.

**Files:**
- Create: `crates/gateway/src/web/auth.rs`, `crates/gateway/src/web/csrf.rs`

**Interfaces:**
- Consumes: `core::user::service`, `core::auth::{password, session}`, `web::WebState`.
- Produces:
  - `GET /login`, `POST /login`, `POST /logout`
  - `csrf::layer()` — middleware rejecting any non-idempotent request whose `_csrf` form field or `X-CSRF-Token` header does not match the `csrf` cookie
  - `csrf::token() -> String`, injected into every rendered template as `csrf_token`

`service::authenticate` must call `password::dummy_verify` when the username is unknown, so an unknown user and a wrong password cost the same. Both return the identical message: `"Invalid username or password."` A suspended user gets that same message — never "your account is suspended", which confirms the account exists.

- [ ] **Step 1: Write the failing tests**

```rust
#[sqlx::test] async fn login_with_valid_credentials_sets_a_session_cookie()  // HttpOnly, SameSite=Lax
#[sqlx::test] async fn login_with_a_wrong_password_renders_the_generic_message()
#[sqlx::test] async fn login_as_a_suspended_user_renders_the_same_generic_message()
#[sqlx::test] async fn login_for_an_unknown_user_still_runs_a_verify()       // Review Focus 2
#[sqlx::test] async fn logout_deletes_the_session_row_and_clears_the_cookie()
#[sqlx::test] async fn a_post_without_a_csrf_token_is_rejected()
#[sqlx::test] async fn a_post_with_a_mismatched_csrf_token_is_rejected()
```

Review Focus 2 is asserted structurally, not by wall-clock timing: spy that the unknown-user path reaches the verify call.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-gateway web::auth`
Expected: FAIL.

- [ ] **Step 3: Implement `auth.rs` and `csrf.rs`**

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-gateway web::auth`
Expected: PASS, 7 tests.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add login, logout and CSRF protection"
```

---

### Task 9: Auth extractors

Owns Review Focus item 1.

**Files:**
- Create: `crates/gateway/src/web/extract.rs`

**Interfaces:**
- Consumes: `core::auth::session::lookup`.
- Produces: `CurrentUser(pub core::user::User)`, `OperatorUser(pub User)`, `AdminUser(pub User)` — all `FromRequestParts`. `CurrentUser` redirects to `/login` when absent, expired, forged or suspended. `OperatorUser` and `AdminUser` return a themed 403.

- [ ] **Step 1: Write the failing tests**

```rust
#[sqlx::test] async fn a_request_with_no_cookie_redirects_to_login()
#[sqlx::test] async fn a_forged_cookie_value_redirects_without_panicking()   // Review Focus 1
#[sqlx::test] async fn an_expired_session_redirects_to_login()
#[sqlx::test] async fn suspending_a_user_invalidates_their_live_session()
#[sqlx::test] async fn a_plain_user_hitting_an_admin_route_gets_403()
#[sqlx::test] async fn an_operator_hitting_an_admin_route_gets_403()
```

The forged-cookie case feeds non-base64 bytes, an over-long value and an empty string.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-gateway web::extract`
Expected: FAIL.

- [ ] **Step 3: Implement `extract.rs`**

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-gateway web::extract`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add session extractors and role guards"
```

---

### Task 10: Profile page

**Files:**
- Create: `crates/gateway/src/web/profile.rs`
- Modify: `crates/gateway/templates/profile.html`

**Interfaces:**
- Consumes: `CurrentUser`, `service::{update_profile, change_password}`.
- Produces: `GET /me`, `POST /me`, `POST /me/password`.

- [ ] **Step 1: Write the failing tests**

```rust
#[sqlx::test] async fn get_me_shows_the_current_users_details()
#[sqlx::test] async fn post_me_updates_the_display_name()
#[sqlx::test] async fn post_me_with_a_taken_username_re_renders_with_a_field_error()
#[sqlx::test] async fn post_me_password_with_a_wrong_current_password_is_rejected()
#[sqlx::test] async fn changing_the_password_keeps_the_user_signed_in()
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-gateway web::profile`
Expected: FAIL.

- [ ] **Step 3: Implement `profile.rs` and finish the template**

Validation errors re-render the form with messages beside the offending input, driven by `Error::Validation(Vec<FieldError>)`.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-gateway web::profile`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add profile editing and password change"
```

---

### Task 11: Admin user management

**Files:**
- Create: `crates/gateway/src/web/admin.rs`
- Modify: `crates/gateway/templates/admin/users.html`

**Interfaces:**
- Consumes: `AdminUser`, `repo::list`, `service::create_user`, `repo::{set_role, set_status}`.
- Produces: `GET /admin/users`, `GET|POST /admin/users/new`, `GET|POST /admin/users/:id`.

- [ ] **Step 1: Write the failing tests**

```rust
#[sqlx::test] async fn an_admin_can_create_a_user()
#[sqlx::test] async fn the_user_list_paginates()
#[sqlx::test] async fn an_admin_can_suspend_and_reactivate_a_user()
#[sqlx::test] async fn an_admin_cannot_suspend_their_own_account()
#[sqlx::test] async fn creating_a_user_with_a_taken_username_shows_a_field_error()
```

Self-suspension is blocked so an admin cannot lock the last administrator out.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-gateway web::admin`
Expected: FAIL.

- [ ] **Step 3: Implement `admin.rs` and finish the template**

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-gateway web::admin`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git commit -am "Add admin user management screens"
```

---

### Task 12: Login rate limiting

**Files:**
- Create: `crates/gateway/src/web/ratelimit.rs`
- Modify: `crates/gateway/src/web/auth.rs` (apply it to `POST /login`)

**Interfaces:**
- Produces: `ratelimit::LoginLimiter::new(max_attempts: u32, window: Duration, lockout: Duration)` with `RetryAfter(pub Duration)`, `check(key: &str) -> Result<(), RetryAfter>` and `record_failure(key: &str)`. Keyed on both the submitted username and the client IP. In-process state is sufficient for one gateway; a comment records that a second replica needs shared state.

- [ ] **Step 1: Write the failing tests**

```rust
fn attempts_under_the_limit_are_allowed()
fn the_limit_triggers_a_lockout()
fn the_lockout_expires_after_the_window()
fn two_usernames_are_limited_independently()
#[sqlx::test] async fn repeated_failed_logins_eventually_return_429()
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p soupcpy-gateway ratelimit`
Expected: FAIL.

- [ ] **Step 3: Implement `ratelimit.rs` and wire it into `POST /login`**

Lockouts are logged with the username and IP, and nothing else — no submitted password, ever.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p soupcpy-gateway ratelimit`
Expected: PASS, 5 tests.

- [ ] **Step 5: Run the whole suite and the full gate**

Run: `cargo fmt -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --workspace`
Expected: all clean; 80-odd tests pass.

- [ ] **Step 6: Commit**

```bash
git commit -am "Rate-limit login attempts"
```

---

### Task 13: CI pipeline

The spec names CI as the mitigation for two risks (§12) and requires a Postgres
service for the test suite (§10). The repo has no `.github/` at all, so those
mitigations are currently fiction. This task makes them real.

**Files:**
- Create: `.github/workflows/ci.yml`, `scripts/check-css.sh`

**Interfaces:**
- Consumes: `scripts/build-css.sh` from Task 7.
- Produces: a workflow running on push and pull request, with a `postgres:16`
  service container and `DATABASE_URL` wired to it.

- [ ] **Step 1: Write the CSS freshness check**

`scripts/check-css.sh` reruns `build-css.sh` into a temporary file and diffs it
against the committed `crates/gateway/assets/app.css`, exiting non-zero on any
difference.

- [ ] **Step 2: Verify it passes on a clean tree and fails on a stale one**

Run: `scripts/check-css.sh`
Expected: exit 0. Then edit a Tailwind class in a template, rerun, and expect a
non-zero exit naming the diff. Revert the edit.

- [ ] **Step 3: Write the workflow**

Four steps: `cargo fmt -- --check`; `cargo clippy --all-targets
--all-features -- -D warnings`; `cargo test --workspace` against the service
container; `scripts/check-css.sh`.

- [ ] **Step 4: Verify the workflow runs green**

Push the branch and confirm all four steps pass. A red run here means the gate in
Global Constraints was not actually being honoured locally.

- [ ] **Step 5: Commit**

```bash
git add .github scripts/check-css.sh
git commit -m "Add CI workflow with Postgres and a CSS freshness check"
```

---

## Definition of done

From spec §14, verified in order:

- [ ] `cargo fmt -- --check`, `cargo clippy --all-targets --all-features -- -D warnings` and `cargo test --workspace` all pass.
- [ ] `docker compose up` brings up Postgres and the gateway; migrations apply; the health check passes.
- [ ] `seed-admin` creates an admin; that admin signs in, edits their profile and password, creates a second user, and signs out.
- [ ] The multiview UI and a live scrcpy stream still work, verified by hand.
- [ ] Every Review Focus item has a passing test in its owning task.
