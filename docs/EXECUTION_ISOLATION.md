# Execution isolation

Highwater runs each deployed application in a warm sandbox pool. The service never imports customer code. A sandbox contains the language runtime, the application bundle, and the Highwater worker; it receives work through a deployment-scoped capability and returns deterministic state transitions.

The reference profile in `deploy/sandbox/worker.yaml` uses a sandboxed OCI runtime such as gVisor. Kata Containers or a Firecracker-backed runtime class can provide the same contract. The profile runs as an unprivileged UID with a read-only root filesystem, no Linux capabilities, no privilege escalation, a runtime-default seccomp profile, bounded CPU and memory, memory-backed temporary storage, no Kubernetes service-account token, no inbound network, and egress limited to the execution endpoint. External API access must be granted explicitly per deployment.

Workers are long-lived and process many activations. Isolation is therefore paid at deployment and scale-out boundaries rather than for every event or microbatch. Pools can scale to zero, retain warm instances according to observed load, and spread replicas across hosts.

The execution token is scoped to one task queue and an allowlist of build IDs. The execution listener exposes polling and completion APIs but not ingestion, deployment, query submission, or administration. The token grants no access to the object journal, checkpoints, RocksDB, ownership records, or cloud credentials. Lease tokens authorize only the completion or renewal of the activation that issued them. Production traffic between sandboxes and the internal execution endpoint must use workload identity and encrypted transport; the token is an additional capability, not a substitute for transport authentication.

The sandbox boundary does not make arbitrary side effects exactly once. Customer code that calls an external system must use Highwater's transactional output delivery or a destination idempotency key.
