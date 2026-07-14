# Redis Client — Blocking command codegen

Standalone build tool that derives the **Lettuce blocking command implementations**
(`LettuceBlockingValueCommandsImpl`, `LettuceBlockingKeyCommandsImpl`, …) from the corresponding
**reactive** command interfaces (`ReactiveValueCommands`, …).

Each generated method is a thin await-wrapper over the reactive one:

```java
return reactive.xxx(args).await().atMost(timeout);
```

## What it does and does not generate

- **Generates** the Lettuce blocking `*Impl` classes only.
- **Does not** generate the blocking command *interfaces* (`ValueCommands`, `KeyCommands`, …). Those
  are hand-curated Quarkus API (curated Javadoc, `throws` clauses, deprecations) and are consumed
  as-is — the generated `*Impl` simply `implements` them.

## Run

```bash
extensions/redis-client/codegen/generate.sh
```

This compiles the generator, runs it, then formats the output (Eclipse profile) and sorts imports.
The script `cd`s into this module first, so it works from any directory. Extra flags are forwarded to
Maven, e.g. `generate.sh -o` for offline.

Output is written to `target/generated/io/quarkus/redis/runtime/client/lettuce/<group>/`. It is
throwaway — **review the diff and copy the classes into
`../runtime/src/main/java/…/lettuce/<group>/`**, then commit them there.

> Run it via the script, not by launching `main` directly from an IDE: the source/output paths are
> relative to this module directory, which the script guarantees as the working directory.

## Configuration

Everything is hardcoded in `BlockingCommandsGenerator`:

| Constant         | Meaning                                                        |
|------------------|---------------------------------------------------------------|
| `COMMAND_GROUPS` | groups to generate (`value`, `keys`, `hash`, …)               |
| `RUNTIME_SRC`    | reactive sources to read (`../runtime/src/main/java`)          |
| `OUT`            | where generated files are written (`target/generated`)        |
| `MANUAL_METHODS` | methods to force to a hand-written stub (see below)           |

**To add a group:** add its name to `COMMAND_GROUPS`. The group's directory under `RUNTIME_SRC` must
contain exactly one `Reactive*Commands.java` (excluding `ReactiveTransactional*`), and the matching
blocking interface must already exist.

## Methods it can't generate

Some methods are not pure await-wrappers and are emitted as `UnsupportedOperationException` stubs with
a `// TODO(codegen)` comment for a human to finish. They are also listed in a summary at the end of the
run. Two cases trigger this:

1. **Structural** — the reactive method does not return `Uni<X>`, so there is nothing to `await()`
   (e.g. `scan()` / `hscan()` return a synchronous cursor). Detected automatically from the return type.
2. **Semantic** — the reactive method returns `Uni<X>` (so it *would* compile as an await-wrapper) but
   the blocking semantics differ (e.g. `blpop` carries a Redis-level blocking timeout distinct from the
   await timeout). The type system can't see this, so list such methods in `MANUAL_METHODS` by their
   `BlockingType#method` key (e.g. `"KeyCommands#blpop"`).

## Guarding against drift

Because the generated `*Impl` classes are committed to the runtime module, they can go stale when a
reactive interface changes without regeneration. Regenerate and diff to catch it:

```bash
extensions/redis-client/codegen/generate.sh
# then compare target/generated against the committed copies under ../runtime/src/main/java
```
