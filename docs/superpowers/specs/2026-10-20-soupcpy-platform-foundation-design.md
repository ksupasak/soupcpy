# soupcpy Platform Foundation — Design

- **Date:** 2026-10-20
- **Status:** approved (design); implementation plan not yet written
- **Scope:** slice 1 of 6 of the platform refactor

## 1. Context

### What exists today

Two standalone Axum crates, neither registered in a workspace, both named `rust`
in their own `Cargo.toml` (the agent was renamed to `soupcpy-agent` in `affb0e5`;
the gateway has not been).

**`soupcpy-agent`** runs at a site, physically next to the handsets. It owns a
local SQLite `channels` table (`name`, `udid`, `group_name`, `code`, `gw`),
administered through a CSV import/export page, dials **out** to the gateway's
`/ws/controller`, and bridges adb/scrcpy traffic up to `/ws/publisher`. It has
no concept of a user. Listener is hardcoded to `0.0.0.0:3001`.

**`soupcpy-gateway`** is the central hub: publisher/receiver/controller/dashboard
WebSocket endpoints, a udid-keyed connection registry, the multiview UI, and the
`ws-scrcpy` bundle served from `public/` via `ServeDir`. It is the deployed piece
(Dockerfile + docker-compose). Critically, it has **no database at all** — no
`sqlx` dependency, no tables, no data volume — and its `session_id` is a socket
session, not a user session. Templates are compiled in with `include_str!`.

Neither crate has any tests.

### What is being asked

A modern admin theme; user login and profile management; an Account model shared
between users with OTP or admin verification; devices, device types, groups,
account sessions and regional device pools; subscription packages with automatic
assignment from a pool and a waiting list; and a USD wallet funded by
operator-approved payment slips, charged monthly, deactivating devices when the
balance runs short.

### Understanding of the business

Remote Android handsets are rented as a service. Customers self-register free,
verify by SMS OTP (or are created by an admin), and create an Account that
several users can share, so a team works on the same devices and balance. They
prepay a USD wallet: choose an amount, receive a transaction number and
receivable details, upload a payment slip; an admin or operator approves it
before the balance moves. Admins can also refill manually, with an audit log.
With balance available they buy a subscription (solo / pack5 / pack10), and the
system assigns **one device at a time per slot** from a **regional pool**
(Thailand, Korea, …). An empty pool puts them on a waiting list. Subscriptions
charge monthly from the wallet automatically.

Success: a customer goes from registration to a live handset without an operator
touching anything except approving the slip.

## 2. Scope

### This slice delivers

Workspace and `core` crate on Postgres with migrations and a real test harness;
the themed admin shell; users with login, sessions, and profile editing (display
name, username, password, email, phone); an admin bootstrap path; admin-created
users.

### Explicitly not this slice

Self-service registration and OTP (inseparable — see §6.1), the Account model and
sharing, device inventory and pools, subscriptions and the waiting list, the
wallet and billing. The agent's behaviour is **unchanged**: it keeps its SQLite
`channels` table and its admin page until the device-inventory slice retires them.

### Roadmap

| Phase | Sub-project | Depends on |
|-------|-------------|------------|
| 0 | Workspace, `core`, Postgres, migrations, test harness | — |
| 1 | Admin shell and theme | 0 |
| 2 | Users: login, sessions, profile | 0, 1 |
| 3 | Accounts: sharing, roles, OTP, registration | 2 |
| 4 | Devices: type, group, regional pools, agent sync | 3 |
| 5 | Subscriptions: packages, assignment, waiting list | 3, 4 |
| 6 | Wallet: top-ups, slip approval, monthly charging | 3, 5 |

Phases 0–2 are this document. Each later phase gets its own spec, plan and
implementation cycle. The theme is built early, as a shell, so later screens land
in it rather than being restyled twice.

## 3. Decisions

**D1. The domain lives in a new `core` crate in a Cargo workspace; the gateway
hosts the portal and admin UI; the agent stays a thin site bridge.**
Users, wallets, subscriptions and a cross-region pool are central, multi-tenant
state. A per-site agent cannot hold them: every site would have its own users and
balance, and no agent can see another region's free devices to assign one. The
workspace layout also reconciles the repo with the structure its own `AGENTS.md`
already describes (`crates/`, a shared crate) but which the code diverges from.
*Rejected:* putting the domain in the agent, as originally asked (breaks pooling
and billing); a third standalone portal service (cleanest boundary, but it must
then query the gateway for live device state — more deployment and integration
for no benefit at this size).

**D2. Postgres.**
Real transactions and row locking for safe device assignment, an exact decimal
type for money, concurrent operators, and room to run more than one gateway.
*Rejected:* SQLite — writes serialise, there is no decimal type, and it rules out
a second gateway replica. *Rejected:* start on SQLite and migrate later — the
migration surfaces its hardest bugs in billing, at the worst possible time.

**D3. Server-rendered minijinja + Tailwind + HTMX, with cookie sessions.**
No Node toolchain. CRUD-heavy operator screens come together fastest this way and
auth stays a plain server-side session.
*minijinja over askama:* askama checks templates at compile time but costs a
recompile per HTML tweak — the same friction `include_str!` has today. minijinja
loads templates at runtime for hot reload in development and embeds them in the
release binary, so Docker stays a single artifact.
*Rejected:* a React SPA (most modern, but a Node toolchain in the Dockerfile and
token-auth plumbing); restyled static HTML (hand-rolled state gets painful across
a dozen screens).

**D4. Money is `numeric(14,2)`.**
Postgres has an exact decimal type, so there is no reason to hand-roll integer
cents. This supersedes earlier advice to use cents, which applied to SQLite.
Not used in this slice; decided now so phase 6 does not relitigate it.

**D5. A failed monthly charge starts a grace period, then releases the device.**
The device goes inactive but stays reserved for a fixed window (default 7 days,
configurable). Top up inside it and the *same* handset returns untouched; miss it
and the device returns to the pool and the subscription ends. For a device farm
the customer's work lives on a specific handset — logged-in accounts, installed
apps, device fingerprint — so immediate release is punitive; but holding
indefinitely lets a non-payer hoard inventory that waiting-list customers are
ready to pay for. Phase 6 implements this; recorded here as settled.

**D6. Device inventory is central; agents sync down.**
`core` owns device, device type, device group and pool. Each agent pulls its own
site's device list from the gateway, and its local `channels` table stops being a
source of truth. One place to administer, and billing can never reference a
device that inventory does not know about. Phase 4 implements this, including
retiring the agent's admin page.
*Rejected:* agent-owned with the gateway mirroring whatever connects (inventory
drifts; a device can vanish mid-subscription).

**D7. Build by strangler, one vertical slice at a time.**
`core` and the portal grow alongside the existing relay, which stays untouched
and keeps running. Each phase is independently deployable.
*Rejected:* build the platform and cut over once — reads faster but leaves weeks
with nothing shippable and risks the one path that must not break.

## 4. Architecture

```
soupcpy-rust/
├─ Cargo.toml              # workspace manifest; versions in [workspace.dependencies]
├─ crates/
│  ├─ core/                # domain. NO axum, NO templates, NO HTTP types
│  │  ├─ src/lib.rs
│  │  ├─ src/db.rs         # PgPool construction, pool options
│  │  ├─ src/error.rs      # core::Error
│  │  ├─ src/user/{mod,repo,service}.rs
│  │  ├─ src/auth/{mod,password,session}.rs
│  │  ├─ src/bin/seed_admin.rs
│  │  ├─ migrations/       # sqlx migrate, embedded via migrate!()
│  │  └─ tests/
│  ├─ gateway/
│  │  ├─ src/main.rs       # thin: config, router assembly, serve
│  │  ├─ src/relay/        # today's registry + fan-out, moved verbatim
│  │  ├─ src/web/{mod,auth,profile,admin,extract,csrf,error}.rs
│  │  └─ templates/
│  └─ agent/               # behaviour untouched this slice
├─ docs/superpowers/specs/
└─ scripts/build-css.sh
```

### Dependency rules

`core` must not depend on `axum`. It exposes services returning domain types and
`core::Error`; `gateway` maps those to HTTP. This is what makes `core` testable
without a server, and the only way the sub-300-line file guidance in `AGENTS.md`
survives six subsystems.

`gateway` depends on `core`. `agent` depends on `core` only from phase 4, for
device identity. Shared versions are declared once in
`[workspace.dependencies]`.

### Crate renames

Both crates are renamed to `soupcpy-gateway` and `soupcpy-agent` (the agent
already was). The Dockerfile's `COPY --from=builder /build/target/release/rust`
and its explanatory comment are replaced, the compose build context moves to the
workspace root, and `start.sh` paths are updated.

### sqlx version

The agent is on `sqlx` 0.7 with the `sqlite` driver; `core` needs `postgres`. The
workspace unifies on a single `sqlx` version carrying both drivers rather than
compiling two copies. Driver features are per-crate; Cargo unions them when
building the workspace, which is acceptable.

## 5. Data model

Only users and sessions this slice, shaped so accounts drop in later without a
rewrite.

```sql
create table users (
  id                bigserial primary key,
  username          text        not null,
  email             text,
  phone             text,
  display_name      text        not null,
  password_hash     text        not null,
  role              text        not null default 'user'
                      check (role in ('user', 'operator', 'admin')),
  status            text        not null default 'active'
                      check (status in ('active', 'suspended')),
  email_verified_at timestamptz,
  phone_verified_at timestamptz,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

create unique index users_username_key on users (lower(username));
create unique index users_email_key    on users (lower(email)) where email is not null;
create unique index users_phone_key    on users (phone)        where phone is not null;

create table sessions (
  token_hash   bytea       primary key,
  user_id      bigint      not null references users(id) on delete cascade,
  created_at   timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  expires_at   timestamptz not null,
  ip           inet,
  user_agent   text
);

create index sessions_user_id_idx on sessions (user_id);
```

### Notes

- **`email` and `phone` are nullable.** An admin-created user may have neither;
  requiring both would block the bootstrap path.
- **Only the session token's hash is stored.** The cookie carries an opaque
  256-bit random value; the table holds its SHA-256. A database leak therefore
  cannot be replayed as a live session. This is also why there is no JWT: server-
  side sessions can be revoked, and revocation matters for a paid service.
- **`role` is CHECK-constrained text, not a Postgres enum**, because altering an
  enum later is painful and more roles are likely.
- **Unique indexes on `lower(...)`** rather than the `citext` extension, to avoid
  an extension dependency.
- **No `account_id` on `users`.** Membership becomes its own table in phase 3,
  since a user will belong to several accounts.
- All timestamps are `timestamptz`, stored UTC.

## 6. Authentication

### 6.1 Why registration is not here

Self-service registration is inseparable from OTP verification: a registration
form that cannot verify anybody is not shippable, and OTP drags in an SMS
provider decision that belongs to phase 3. So this slice ships login, and users
come from the bootstrap path and from admins.

### 6.2 Admin bootstrap

`core` ships a `seed-admin` binary that creates or promotes one admin from
environment variables (`SEED_ADMIN_USERNAME`, `SEED_ADMIN_PASSWORD`). It is
idempotent: run twice and the second run updates the password rather than
failing. The password is read from the environment, never a CLI argument, so it
does not land in shell history or the process list.

### 6.3 Passwords

argon2id, not bcrypt. Parameters are pinned in `core::auth::password` with a
comment recording the tuning target (roughly 100ms on the deployment host) so
they can be raised deliberately. The hash string embeds its parameters, so
rehash-on-login handles future changes.

### 6.4 Sessions

Opaque 256-bit random token, SHA-256 hashed at rest. Cookie is `HttpOnly`,
`SameSite=Lax`, `Secure` (configurable off for plain-HTTP local development),
with sliding expiry — `last_seen_at` refreshes on use — under an absolute cap
(default 30 days). Logout deletes the row. Login rejects any user whose `status` is not
`active`, returning the same generic failure message as a wrong password so the
form does not disclose which accounts exist. Expired rows are swept by a periodic
task, wrapped in a `tracing::info_span!` per the `AGENTS.md` runtime guidance.

### 6.5 CSRF

Because auth is cookie-based, **every mutating form carries a CSRF token**
(per-session, double-submit: a cookie plus a hidden field or an `HX-` header that
must match). HTMX partial posts make this easy to forget, so the check lives in
one middleware rather than per handler, and a test asserts a mismatched token is
rejected.

### 6.6 Rate limiting

Login is limited per username and per IP, with a short lockout after repeated
failures. The purpose is to make credential stuffing expensive, not to be a full
WAF.

### 6.7 Extractors

`CurrentUser` resolves the session or redirects to `/login`. It also rejects a
user suspended since the session was issued, deleting that session, so suspending
someone takes effect immediately rather than whenever their cookie expires. `RequireRole`
guards operator and admin routes and returns a themed 403. Both live in
`gateway::web::extract`.

### 6.8 Logging

No credential, token, session value or password hash is ever logged, at any
level. `af8ebab` was a log-leakage fix, so this is a live concern in this
codebase, not a hypothetical.

### 6.9 Profile editing

Display name, username, email and phone are editable. A password change requires
the current password. **Changing email or phone clears the corresponding
`verified_at`**, so phase 3 re-verifies it rather than trusting a value that was
verified for a different address.

## 7. HTTP surface

New, all server-rendered:

| Method | Path | Purpose |
|--------|------|---------|
| GET/POST | `/login` | Form and submit |
| POST | `/logout` | Destroy session |
| GET/POST | `/me` | Profile |
| POST | `/me/password` | Change password |
| GET | `/admin/users` | List (paginated) |
| GET/POST | `/admin/users/new` | Create |
| GET/POST | `/admin/users/:id` | Edit, suspend, set role |
| GET | `/assets/*` | Built CSS and JS |

Untouched — this is the strangler boundary: `/`, `/multiview`, `/stream`,
`/health`, `/ws`, `/ws/publisher`, `/ws/controller`, `/ws/dashboard`,
`/api/settings`, `/api/channels`, and `ServeDir("public")` with the `ws-scrcpy`
bundle. The relay keeps working throughout, and nothing in this slice touches its
code paths beyond moving them into `src/relay/` unchanged.

## 8. UI shell

Sidebar plus topbar with a user menu, light and dark, responsive, flash
messages. HTMX handles inline forms and partial swaps; full page loads remain the
fallback so the UI works without JavaScript for the auth paths.

**Tailwind without Node:** the standalone Tailwind CLI binary, driven by
`scripts/build-css.sh`, with the built `app.css` committed. The tradeoff is an
occasional stale-CSS commit; the alternative is Node in the Dockerfile, which was
ruled out. CI runs the script and fails if the committed CSS differs.

Templates live in `crates/gateway/templates/`, loaded from disk in development
and embedded in the binary for release.

## 9. Error model

`core::Error` is an enum: `NotFound`, `Conflict`, `Unauthorized`, `Validation`,
`Db`. The gateway maps it to a status plus either a themed error page or an HTMX
partial. This replaces the `Box<dyn std::error::Error + Send + Sync>` the current
`run()` returns, which cannot be matched on and so cannot drive a status code.

Validation errors carry field-level detail so forms can re-render with messages
next to the offending input.

## 10. Testing

The harness is part of the deliverable: there are **zero tests today**, and every
later phase inherits whatever this slice establishes.

- **`core`:** `#[sqlx::test]` gives each test its own throwaway Postgres
  database — real SQL, no mocks, no shared-state flake. Happy and failure path
  per service, per `AGENTS.md`.
- **`gateway`:** router-level tests via `tower::ServiceExt::oneshot` with a
  cookie jar.
- **Named cases that must exist:** login with a wrong password; login against a
  suspended user; an expired session; a CSRF token mismatch; a non-admin hitting
  an admin route; a duplicate username differing only in case; changing email
  clears `email_verified_at`; `seed-admin` run twice.
- CI needs a Postgres service and `DATABASE_URL`.

## 11. Deployment and configuration

Compose gains a `postgres` service with a named volume; `DATABASE_URL` comes from
an ignored `.env`, per the `AGENTS.md` instruction to keep secrets out of the
repo. Migrations run at gateway startup via `sqlx::migrate!()`, logged, and the
process exits non-zero if they fail.

The gateway's config is being moved to environment variables by `63f10f0`
(landing on `fix/gateway-websocket-session-routing`); the database URL and session
settings follow that pattern rather than inventing a second mechanism.

**Blocker:** `docker-compose.yml` is pinned to `network_mode: bridge` with a
comment recording that this host "has exhausted its network address pools (33
networks), and a dedicated network fails to create", and that those lines must go
if a second service needs DNS resolution by service name. Postgres is exactly
that second service. `docker network prune` must happen before this phase, or the
database will not come up.

## 12. Risks

| Risk | Mitigation |
|------|-----------|
| The workspace move touches every path in the repo while another worker is committing in the same checkout | Land their branch first, or do phase 0 in a separate worktree. Unresolved — see §13. |
| Docker network pools exhausted, so Postgres cannot join compose | `docker network prune` before phase 0; verify a second service resolves by name. |
| Breaking the relay while restructuring the gateway | `src/relay/` is a move, not a rewrite; relay routes are covered by a smoke test before and after. |
| Committed Tailwind CSS goes stale | CI rebuilds and fails on a diff. |
| argon2 parameters too weak or too slow on the deployment host | Tune on that host, pin with a comment, rehash on login. |

## 13. Open questions

1. **Checkout coordination.** Merge `fix/gateway-websocket-session-routing`
   first, or do the repo-wide move in a separate worktree? Phase 0 cannot start
   safely until this is answered.
2. **Session absolute cap** — 30 days proposed; confirm against how long
   customers expect to stay signed in.
3. **Secure-cookie toggle** — is the portal always behind TLS in production? If
   so the flag can be hardcoded on rather than configurable.

## 14. Definition of done

- The workspace builds; `cargo fmt --check`, `cargo clippy --all-targets
  --all-features -- -D warnings` and `cargo test --workspace` all pass.
- `docker compose up` brings up Postgres and the gateway; migrations apply; the
  health check passes.
- `seed-admin` creates an admin; that admin signs in, edits their profile and
  password, creates a second user, and signs out.
- The multiview UI and a live scrcpy stream still work, verified manually.
- Every named test case in §10 exists and passes.
