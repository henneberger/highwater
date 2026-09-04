# Production Kubernetes deployment

This directory defines the hosted Highwater boundary without a provider-specific release system.

Render a deployment from immutable images:

```shell
python deploy/kubernetes/render.py \
  --server-image ghcr.io/henneberger/highwater-server@sha256:SERVER_DIGEST \
  --application-image ghcr.io/acme/shopping@sha256:APPLICATION_DIGEST \
  --application-module shopping_app \
  --process shopping-assistant \
  --task-queue shopping-production \
  --journal s3://highwater-production/journal \
  --output hosted.production.yaml
kubectl apply --dry-run=server -f hosted.production.yaml
kubectl apply -f hosted.production.yaml
```

The renderer rejects mutable image tags, invalid workload names, invalid journal URIs, and unresolved template values.

Publish multi-architecture server and worker images from the local machine with `scripts/publish-images.sh VERSION`. The script requires `GHCR_TOKEN` with `write:packages`, emits provenance and SBOM attestations, and prints the registry manifests. Resolve the published tags to their platform-index digests before rendering the deployment.

Before applying the rendered file:

1. publish immutable server and application-worker images;
2. create `highwater-api-token`, `highwater-cluster-token`, and `highwater-execution-identities` through the cluster secret manager;
3. create `highwater-worker-identity` with the execution token authorized by the identity file;
4. attach object-journal access to the `highwater-core` service account through the cloud workload-identity mechanism;
5. install the `gvisor` RuntimeClass on worker nodes;
6. terminate public TLS at a gateway that routes only to `highwater-public:7233`.

The two core deployments are overlapping candidates for all four durable process
partitions. Conditional S3 journal updates select one owner per partition; the
other core can take over after its lease expires. Both cores run control
maintenance, and the public Service selects both. Local RocksDB and checkpoint
directories are disposable caches. This provides failover eligibility, not
automatic balanced partition placement: the first core may initially own all work.

Each core has a worker pool pinned to its private execution Service. Both pools
poll all four partitions; a standby returns no work until ownership changes.
Completions and renewals therefore reach the issuing core. Do not point these
workers at the aggregate `highwater-execution` Service: per-request load balancing
does not preserve that routing. Keep at least one worker in each pool; the
autoscalers enforce that minimum.

Core pods and the two worker pools use host anti-affinity. Provide at least two
eligible hosts with the required gVisor runtime and enough surviving capacity for
the full workload. Core deployments use `Recreate` so a stable per-core Service
does not route across two simultaneous incarnations during rollout. Pod UID-based
identities prevent a returning incarnation from treating a surviving old process
as itself. Startup probes allow up to ten minutes for recovery before liveness
checks restart the pod. Roll core deployments one at a time and wait for the
replacement to become ready before changing the other; the PDB does not serialize
independent Deployment rollouts.

The new live-worker MinIO drill kills each core in turn while publishing identified
events, starts a replacement from empty local state, and verifies exact state after
both failovers. See [HA validation](../../docs/HIGH_AVAILABILITY.md) for the guarantee
boundary and reproduction command. No cluster deployment is performed by rendering.

The manifest is a reference topology. Run the release-readiness tests against the exact cluster, object store, gateway, regions, resource profiles, and application image used for production.
