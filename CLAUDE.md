# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Fabric-X Orderer is the ordering service of Fabric-X, implementing the **Arma** BFT protocol
("Arma: a scalable Byzantine Fault Tolerant ordering service", https://ia.cr/2024/808). Arma
scales BFT ordering horizontally by separating transaction dissemination and persistence from
consensus: consensus orders only compact batch *metadata*, while full payloads flow through a
parallel, shardable data plane. The client-facing API is Fabric's Atomic Broadcast API
(`Broadcast`/`Deliver`), so standard Fabric clients are largely compatible.

## The four node roles (the core mental model)

Everything maps to four server roles, each a top-level package under `node/`. The end-to-end
flow is: **Router → Batcher → Consenter → Assembler**.

- **Router** (`node/router`) — party-local entry point. Accepts client `common.Envelope`s via the
  `Broadcast` API, applies request checks, maps each TX to a shard (CRC64 over the payload, so all
  routers route identically), and forwards to the batcher of its own party in that shard.
- **Batcher** (`node/batcher`) — grouped into **shards**. Bundles TXs into batches, persists and
  replicates them within the shard (one *primary* per shard/term; *secondaries* pull-verify-ack),
  and sends compact **Batch Attestation Fragments (BAFs)** — a signed digest + metadata — to the
  consenters. Payloads stay in the batcher; they never cross the consensus path.
- **Consenter** (`node/consensus`) — runs SmartBFT over BAFs and control events, emitting a total
  order of **Batch Attestations (BAs)**. Also the system controller (e.g. changing a shard's
  primary on enough complaints).
- **Assembler** (`node/assembler`) — collates the ordered BA stream from consensus with full
  batches pulled from batchers, materializing a Fabric-compatible **block ledger**. Serves blocks
  to consumers via the `Deliver` API.

A **party** runs one router, one consenter, one assembler, and one batcher per shard. Multiple
parties run the same roles and cooperate for BFT and censorship resistance.

Read `docs/architecture.md` first for the full flow (single-party then multi-party); it is the
authoritative overview. `docs/api.md` covers the client API.

## Two binaries

- `arma` (`cmd/arma`) — the node binary. Subcommands `router | batcher | consensus | assembler`,
  each taking a mandatory `--config=<file>.yaml`. The CLI wiring and per-role launch/bootstrap
  sequence live in `node/server/arma.go` (`launchRouter`, `launchBatcher`, etc.).
- `armageddon` (`cmd/armageddon`) — network config generator and test/load client. `generate`
  produces per-node YAML config + crypto material from a template; `submit`/`load`/`receive`
  drive traffic. See `cmd/armageddon/README.md` for all subcommands and flags.

## Common commands

```bash
make binary            # builds ./bin/arma and ./bin/armageddon
make linter            # golangci-lint (config in .golangci.yml) + dep/help-doc checks
make protos            # regenerate protobuf artifacts from .proto files
make basic-checks      # license headers, DCO, imports, protos, linter — run before pushing
make build-image       # build the Docker image locally (Docker or Podman)
```

Commits must be signed off (`git commit -s`) — `make check-dco` enforces DCO. All source files
need the Apache license header (`make check-license`).

## Tests

Unit and integration tests are split into Make targets because some suites are slow and run with
`-race`:

```bash
make unit-tests                    # unit-tests-other + -consensus + -batcher
make unit-tests-other              # everything except node/consensus, node/batcher, and /test
make unit-tests-consensus          # node/consensus (skips TestConsensusFullReplacement)
make unit-tests-batcher            # node/batcher
make integration-tests             # test/basic + test/faulttolerance + test/reconfig
```

Integration/e2e suites live under `test/` (`basic`, `faulttolerance`, `reconfig/*`, `chaos`) and
spin up real multi-node networks. Test helpers (network setup, MSP/crypto gen, TLS, stub nodes,
port allocation) live in `testutil/`.

Run a single test:

```bash
go test -race -run '^TestName$' ./node/router/...
go test -race -run '^TestConsensusFullReplacement$' ./node/consensus/...   # excluded from default target
```

Tests use `testify` and (especially integration suites) `ginkgo`/`gomega`. `ginkgolinter` is
enabled, so follow gomega idioms.

## Configuration model

Config splits into **local** and **shared** (`config/`):

- **Local config** — per-node YAML: identity (MSP dir/ID, BCCSP), listen addresses, file-store
  paths, and a **bootstrap** section.
- **Shared config** — the network-wide topology (parties, shards, endpoints, consensus params),
  represented as an `ordererpb.SharedConfig` and carried in a **config block**.

`config.ReadConfig` (entry point) loads local config, then obtains shared config via the bootstrap
`Method`: `yaml` (read a shared-config file directly) or `block` (bootstrap from a genesis/config
block; routers and batchers may resume from the last block in their `configstore`). A node then
calls `Extract<Role>Config(lastConfigBlock)` to get its role-specific runtime config.

Every node must be given the **same genesis block**; a node fails to start without it.
Reconfiguration (membership, identity, endpoints, params) flows through new config blocks — see
`config/reconfig.go` and the `test/reconfig/*` suites.

## Where things live

- `node/comm` — gRPC servers/clients, cluster service, TLS/mutual-TLS auth, deliver plumbing.
- `node/ledger`, `common/ledger` — block/batch ledger storage (goleveldb-backed).
- `node/delivery`, `common/deliver`, `common/deliverclient` — Deliver API server + client.
- `common/types` — core domain types shared across roles (batches, BAFs, batched requests).
- `request/` — router-side request pool/batching internals (pending, buckets, batch store).
- `common/` — cross-cutting infra: `policy`, `msputils`, `requestfilter`, `monitoring`,
  `operations` (ops/metrics endpoint), `synchronizer` (shared state-sync), `configstore`.
- Protobuf definitions live under `node/protos/` and `node/comm/`; regenerate with `make protos`,
  never hand-edit generated `.pb.go` files. `check-protos` verifies they are current.
- Dependencies are **vendored** (`vendor/`); `make check-deps` flags unused vendored deps.

## Conventions

- Go 1.26. Depends heavily on `hyperledger/fabric-x-common`, `hyperledger/fabric-lib-go` (MSP,
  BCCSP, flogging), `fabric-protos-go-apiv2`, and `hyperledger-labs/SmartBFT` (consensus engine).
- Logging is `flogging` (`FabricLogger`), typically named per role/party
  (e.g. `Batcher%dShard%d`).
- Errors use `github.com/pkg/errors` (wrap with context).
- The linter set is strict (`.golangci.yml`): `gosec`, `gocritic`, `revive`, `ireturn`, `lll`,
  `gocognit`, etc. `make linter-extra` runs additional checks against `main` for new changes;
  `golangci-lint run --fix --new-from-rev=main` fixes the auto-fixable ones.
- CLI help text is checked against docs (`make check-help-docs` / `scripts/help_docs.sh`); if you
  change a command's flags/help, regenerate the docs.

### Coding guidelines

- **Simplicity first** — prefer concrete types over interfaces (the `ireturn` linter enforces
  this), reserve generics for `utils/`-style plumbing, favor explicit over clever.
- **Code organization** — collate a struct's methods together, and place a method above the
  functions it calls (caller before callee).
- **Reuse** existing helpers in `common/`, `node/utils`, and `fabric-x-common` before adding new
  ones.

## Performance

Arma is a throughput-oriented system; treat the hot paths (router request pool, batching,
assembler collation) with care. Profile before optimizing and validate assumptions with
benchmarks rather than guessing.

## Before opening a PR

Run `make basic-checks` (license, DCO, imports, protos, linter) plus the unit-test target(s)
covering your change. **Don't run the full test suite locally — it's slow by design** (that's why
the targets are split); rely on CI (`.github/workflows/verify-build.yml`) for the complete run and
monitor it. Commits must be signed off (`git commit -s`).
