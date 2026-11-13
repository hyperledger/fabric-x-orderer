# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Fabric-X Orderer is the ordering service of Fabric-X, implementing the **Arma** BFT protocol
("Fabric-X: Scaling Hyperledger Fabric for Asset Exchange", https://dl.acm.org/doi/10.1145/3788853.3803092).
Arma scales BFT ordering horizontally by separating transaction dissemination and persistence from
consensus: consensus orders only compact batch *metadata*, while full payloads flow through a
parallel, shardable data plane. The client-facing API is Fabric's Atomic Broadcast API
(`Broadcast`/`Deliver`, see https://github.com/hyperledger/fabric), so standard Fabric clients
are largely compatible.

## The four node roles (the core mental model)

Everything maps to four server roles, each a top-level package under `node/`. The end-to-end
flow is: **Router → Batcher → Consenter → Assembler**.

- **Router** (`node/router`) — party-local entry point. Accepts client `common.Envelope`s via
  `Broadcast`, maps each TX to a shard (CRC64 over the payload, so all routers route identically),
  and forwards it to its own party's batcher in that shard.
- **Batcher** (`node/batcher`) — grouped into **shards**. Bundles TXs into batches, persists and
  replicates them within the shard (one *primary* per shard/term; *secondaries* pull-verify-ack),
  and sends **Batch Attestation Fragments (BAFs)** — signed digest + metadata — to the consenters.
  Payloads never cross the consensus path.
- **Consenter** (`node/consensus`) — runs SmartBFT (https://github.com/hyperledger/SmartBFT) over
  BAFs and control events, emitting a total order of **Batch Attestations (BAs)**. Also the system
  controller (e.g. changing a shard's primary on enough complaints).
- **Assembler** (`node/assembler`) — joins the ordered BA stream with full batches pulled from
  batchers into a Fabric-compatible **block ledger**, served via `Deliver`.

A **party** runs one router, one consenter, one assembler, and one batcher per shard. Multiple
parties run the same roles and cooperate for BFT and censorship resistance.

Read `docs/architecture.md` first for the full flow (single-party then multi-party); it is the
authoritative overview. Per-role detail is in `docs/{batcher,consensus,assembler}.md`; `docs/api.md`
covers the client API, `docs/cli/` the generated CLI reference, `docs/monitoring/` metrics.

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
make linter            # goimports, gofumpt, go vet, staticcheck + dep/help-doc checks (builds bin/)
make linter-extra      # golangci-lint (.golangci.yml) on changes vs main
make protos            # regenerate protobuf artifacts from .proto files
make basic-checks      # license headers, DCO, imports, protos, linter, version injection
make build-image       # build the Docker image locally (Docker or Podman)
```

`make linter` needs `goimports`, `gofumpt`, and `staticcheck` on `PATH` (see the `go install` steps
in `.github/workflows/verify-build.yml`).

## Tests

Unit and integration tests are split into Make targets because some suites are slow and run with
`-race`:

```bash
make unit-tests                              # unit-tests-other + -consensus + -batcher
make unit-tests-other                        # everything except node/consensus, node/batcher, and /test
make unit-tests-consensus                    # node/consensus (skips TestConsensusFullReplacement)
make unit-tests-consensus-full-replacement   # just TestConsensusFullReplacement (own CI job)
make unit-tests-batcher                      # node/batcher
make integration-tests                       # integration-basic + -faulttolerance + -reconfig
make integration-reconfig-<membership|identity|endpoints|params|configtx>
```

`integration-reconfig-membership` skips `TestPartiesFullReplacement` and `TestReJoinSingleParty`;
they run as their own targets/CI jobs (`integration-reconfig-membership-full-parties-replacement`,
`integration-reconfig-membership-rejoin-single-party`).

Integration/e2e suites live under `test/` (`basic`, `faulttolerance`, `reconfig/*`,
`deterministic-failure-test`, `fully-randomized-failure-test`) and spin up real multi-node
networks. Test helpers (network setup, MSP/crypto gen, TLS, stub nodes, port allocation) live in
`testutil/`.

Run a single test:

```bash
go test -race -run '^TestName$' ./node/router/...
```

Write new tests with the standard `testing` package plus `testify/require`. Some integration
suites use `ginkgo`/`gomega` (`ginkgolinter` is on, so follow gomega idioms when touching them);
do not write new ginkgo/gomega tests.

Mocks are generated (counterfeiter / mockery via `//go:generate`); after changing an interface,
run `go generate` in that package rather than editing mocks by hand.

## Configuration model

Config splits into **local** and **shared** (`config/`):

- **Local config** — per-node YAML: identity (MSP dir/ID, BCCSP), TLS, listen addresses and ports,
  file-store paths, a bootstrap section, and role-specific parameters.
- **Shared config** — the network-wide topology (parties, shards, endpoints, consensus params),
  represented as an `ordererpb.SharedConfig` and carried in a **config block**.

`config.ReadConfig` (entry point) loads local config, then obtains shared config via the bootstrap
`Method`. The only supported method is `block`: bootstrap from a genesis/config block; routers and
batchers may resume from the last block in their `configstore`, consenters and assemblers from
their ledger. A node then calls `Extract<Role>Config(lastConfigBlock)` to get its role-specific
runtime config.

Every node must be given the **same genesis block**; a node fails to start without it.
Reconfiguration (membership, identity, endpoints, params) flows through new config blocks — see
`config/reconfig.go` and the `test/reconfig/*` suites.

## Where things live

- `node/comm` — gRPC servers/clients, cluster service, TLS/mutual-TLS auth, deliver plumbing.
- `node/ledger`, `common/ledger` — block/batch ledger storage (block files, with a Pebble block index).
- `node/delivery`, `common/deliver`, `common/deliverclient` — Deliver API server + client.
- `common/types` — core domain types shared across roles (batches, BAFs, batched requests).
- `request/` — the batcher's router-side request pool/batching internals (pending, buckets, batch store).
- `common/` — cross-cutting infra: `policy`, `msputils`, `requestfilter`, `monitoring`,
  `operations` (ops/metrics endpoint), `fabhttp` (HTTP server for it), `synchronizer` (shared
  state-sync), `configstore`, `configack` (config-ack sender/receiver to consenters), `ratelimit`
  (GCRA token-bucket limiter), `filerepo`, `metadata` (version vars injected via ldflags).
- Protobuf definitions live under `node/protos/{comm,state}`; regenerate with `make protos`,
  never hand-edit generated `.pb.go` files. `check-protos` verifies they are current.
- Dependencies are **vendored** (`vendor/`); `make check-deps` flags unused vendored deps.

## Conventions

- Go 1.27. Depends heavily on `hyperledger/fabric-x-common`, `hyperledger/fabric-lib-go` (MSP,
  BCCSP, flogging), `fabric-protos-go-apiv2`, and `hyperledger/SmartBFT` (consensus engine).
- Logging is `flogging` (`FabricLogger`), typically named per role/party
  (e.g. `Batcher%dShard%d`).
- Errors: use the standard library (`errors`, `fmt.Errorf` with `%w`, `errors.Is`/`errors.As`,
  `errors.Join`). `github.com/pkg/errors` is archived and being removed.
  - Policy: don't add new uses of `pkg/errors`, but don't migrate existing code unrelated to your
    change either (it bloats the PR review). Use `github.com/cockroachdb/errors` only where stack
    traces are actually needed.
  - Aliasing: in a file that still imports `pkg/errors`, `fmt.Errorf` needs no alias, and
    `errors.Is`/`As`/`Unwrap` already forward to the standard library. Alias only when you need the
    standard library's `errors.New` or `errors.Join`, and then always as
    `pkgerrors "github.com/pkg/errors"`.
  - Mapping, for code you do migrate:
    - `errors.Wrap(err, "msg")`, `errors.WithMessage(err, "msg")` → `fmt.Errorf("msg: %w", err)`
    - `errors.Wrapf(err, "f %d", x)`, `errors.WithMessagef(…)` → `fmt.Errorf("f %d: %w", x, err)`
    - `errors.WithStack(err)` → `err` (just drop the wrapper)
    - `errors.Errorf(…)` → `fmt.Errorf(…)`
  - `nil` trap: every `pkg/errors` wrapper (`Wrap*`, `WithMessage*`, `WithStack`) returns `nil`
    for a `nil` error, but `fmt.Errorf("…: %w", nil)` does not. Add an explicit `if err != nil`
    wherever the old code relied on that (e.g. `return errors.WithStack(f.Close())`).
- `.golangci.yml` enables a wider linter set than `make linter` (`gosec`, `gocritic`, `revive`,
  `ireturn`, `lll`, `gocognit`, …), run only by `make linter-extra`; keep new code clean under it.
  CI does not run it, and the config is in golangci-lint v1 format, so it needs a v1 binary.
  `golangci-lint run --fix --new-from-rev=main` fixes the auto-fixable ones.
- CLI help text is checked against `docs/cli/` (`make check-help-docs`); if you change a command's
  flags/help, run `./scripts/help_docs.sh generate` to regenerate the docs.
- All source files need the Apache license header (`make check-license`).

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

- Commits must be signed off (`git commit -s`); `make check-dco` enforces it.
- Run `make basic-checks` plus the unit-test target(s) covering your change. **Don't run the full
  suite locally — it's slow by design**; rely on CI (`.github/workflows/verify-build.yml`) for the
  complete run and monitor it.
