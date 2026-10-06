# Admission tasks

- [x] Preserve the pre-implementation contract and observed vertical RED/GREEN logs.
- [x] Provide opaque expected-byte one-key API without changing existing methods.
- [x] Prove exact empty/absent behavior, no-op/conflict effects, failure carriers,
  ordinary-format replay, same-key race, byte-ABA scope and existing batch gate.
- [x] Verify actual temporary file no-op bytes, rotation, online compaction,
  Buffered/Physical reopen, 159 synthetic torn-event cases and Physical ACK
  followed by actual process SIGKILL/reopen.
- [ ] Freeze final fmt/clippy/full-suite logs and source hashes.
- [ ] Complete independent final review and upstream admission.
- [ ] Land/publish a clean reviewed dependency-main change; record its real SHA.
- [ ] Move consumer pins in a separate admitted change with downstream checks.
- [ ] Complete target performance/durability/rollout evidence before deployment.

Checked items describe isolated preparation only. No published API, production
installation, native receipt implementation or historical incident resolution
is implied.
