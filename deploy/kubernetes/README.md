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

The two core deployments own disjoint durable partitions. Both use the same conditional S3 journal. Local RocksDB and checkpoint directories are disposable caches. Application workers reach only the private execution service. The autoscaler watches one Process and updates the worker Deployment through the Kubernetes scale subresource.

The manifest is a reference topology. Run the release-readiness tests against the exact cluster, object store, gateway, regions, resource profiles, and application image used for production.
