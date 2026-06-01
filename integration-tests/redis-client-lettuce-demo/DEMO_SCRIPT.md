# Redis Client Lettuce Backend — Demo Script

End-to-end walkthrough for the recorded demo (~5 min). Same demo app, two
Redis client backends (Vert.x and Lettuce), one Redis, server-side proof via
RedisInsight. Each step is tagged with the surface it happens on: **[console]**
(terminal at repo root), **[browser]** (demo UI at <http://localhost:8080>),
**[insight]** (RedisInsight, connected to `127.0.0.1:6379`).

## Prerequisites

Docker running, RedisInsight installed, web browser, terminal at the
repository root.

## Setup — pick a mode

### Option A — JVM (default, ~30 s build)

**[console]**
```bash
docker run -d --name redis-demo -p 6379:6379 redis:7-alpine
./integration-tests/redis-client-lettuce-demo/run-demo.sh
```

### Option B — Native (~80 s build, ~30 ms startup)

One-time toolchain install (Mandrel 25, macOS aarch64 shown — Linux/Windows
users pick the matching asset from
<https://github.com/graalvm/mandrel/releases/latest>):

**[console]**
```bash
mkdir -p ~/.local/mandrel && cd ~/.local/mandrel && \
  curl -sL -o mandrel.tar.gz \
    https://github.com/graalvm/mandrel/releases/download/mandrel-25.0.3.0-Final/mandrel-java25-macos-aarch64-25.0.3.0-Final.tar.gz && \
  tar -xzf mandrel.tar.gz && rm mandrel.tar.gz && cd -
```

Then each run:

**[console]**
```bash
docker run -d --name redis-demo -p 6379:6379 redis:7-alpine
export GRAALVM_HOME=$HOME/.local/mandrel/mandrel-java25-25.0.3.0-Final/Contents/Home
./integration-tests/redis-client-lettuce-demo/run-demo.sh --native
```

### After the script prints "Demo running"

**[browser]** open <http://localhost:8080> — both panels render side-by-side.

**[insight]** open the database for `127.0.0.1:6379`.

## Act 1 — Framing

**[browser]** Show the two panels: **vertx · 8080** and **lettuce · 8081**.

> "Same application code, same Redis, two client backends — selected at build
> time. We'll drive them through the same REST surface, show they're
> behaviorally identical, then look at server-side fingerprints."

## Act 2 — Basic ops (~60 s)

**[browser]** On the **Vert.x** panel click **set** → response shows `OK`.

**[browser]** On the **Lettuce** panel click **get** → shows the value just
written by Vert.x. Reverse the direction once.

**[browser]** Alternate **incr** clicks across both panels — the counter
climbs regardless of backend.

**[insight]** Browser → refresh. Point at `demo:key` and `demo:counter`.

> "Single keyspace, both clients see it."

## Act 3 — `withConnection` pinning (~90 s, headline act)

**[browser]** On both panels click **run /with-connection/client-ids**.
Expected:

- **Lettuce**: `inside_1 == inside_2` ✓ and `inside != outside` ✓ (both green).
- **Vert.x**: `inside_1 == inside_2` ✓ (green) and `inside != outside` ✗ (red).

> "The contract is that the callback sees an **exclusive connection** for its
> entire body — that's `inside_1 == inside_2`, green on both. The
> `inside != outside` part is a side-effect of pool model. Lettuce explicitly
> opens a new `StatefulRedisConnection` for the pinned block, so it shows a
> different id from the shared multiplexed one. Vert.x's pool is lazy: at
> near-zero load only one connection exists, so pinned and ambient happen to
> be the same socket. Pinning is still correct — there's nothing in the pool
> to contrast with."

## Act 4 — Nested `withConnection` (~30 s)

**[browser]** On both panels click **run /with-connection/nested**. Expected
`outer == inner` on both, both green.

> "Nested `withConnection` calls reuse the outer connection — recursion guard
> works identically on both backends."

## Act 5 — Server-side observability (~90 s)

**[insight]** Workbench → run:

```
CLIENT LIST
```

Walk through the rows. Expected categories:

| `name=` / `lib-name=` | what it is |
|------------------------|------------|
| `name=redisinsight-common-…` | RedisInsight dashboard polling |
| `name=redisinsight-browser-…` | RedisInsight key browser |
| `name=redisinsight-cli-…` | this Workbench query |
| `flags=O cmd=monitor` | Profiler (if open) |
| `lib-name=Lettuce 6.5.5.RELEASE` | demo `:8081` |
| empty `lib-name`, RESP3, not a `redisinsight-…` name | demo `:8080` |

> "`lib-name=Lettuce` is the unambiguous tag — Lettuce sends `CLIENT SETINFO`
> on connect. Vert.x's client doesn't, so the second demo connection shows up
> by elimination. Same Redis instance (`laddr` identical), two distinct client
> libraries."

Optional sub-beat:

**[insight]** Profiler → **Start Profiler**.
**[browser]** Click **incr** on each demo panel.
**[insight]** The two `INCR demo:counter` lines come from different client
ports. Stop the profiler.

## Act 6 — Cleanup (~10 s)

**[browser]** Click **flushall** on either panel.
**[insight]** Refresh Browser — keyspace empty.

## Teardown

**[console]** `Ctrl-C` in the `run-demo.sh` terminal, then:

```bash
docker rm -f redis-demo
```

## Talking points reference card

- Same app, two backends, one Redis.
- `withConnection` contract: `inside_1 == inside_2` (callback sees an exclusive connection).
- Lettuce: multiplexed shared connection + per-pin new socket. Vert.x: lazy pool, one connection on this load.
- Server-side fingerprint: `lib-name=Lettuce` vs empty (Vert.x). Nested pinning honoured on both.
