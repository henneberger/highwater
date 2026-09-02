# Production Kubernetes deployment

This directory defines the hosted Highwater boundary without a provider-specific release system.

Before applying it:

1. publish immutable server and application-worker images;
2. replace `IMAGE_TAG`, `APPLICATION_IMAGE`, and `s3://HIGHWATER_JOURNAL_BUCKET/production`;
3. create `highwater-api-token`, `highwater-cluster-token`, and `highwater-execution-identities` through the cluster secret manager;
4. create `highwater-worker-identity` with the execution token authorized by the identity file;
5. attach object-journal access to the `highwater-core` service account through the cloud workload-identity mechanism;
6. install the `gvisor` RuntimeClass on worker nodes;
7. terminate public TLS at a gateway that routes only to `highwater-public:7233`.

The two core deployments own disjoint durable partitions. Both use the same conditional S3 journal. Local RocksDB and checkpoint directories are disposable caches. Application workers reach only the private execution service. The autoscaler watches one Process and updates the worker Deployment through the Kubernetes scale subresource.

The manifest is a reference topology. Run the release-readiness tests against the exact cluster, object store, gateway, regions, resource profiles, and application image used for production.
