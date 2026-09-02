from __future__ import annotations

import json
import os
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")


@dataclass(frozen=True)
class DeploymentScale:
    replicas: int
    resource_version: str


class KubernetesScaleClient:
    def __init__(
        self,
        endpoint: str,
        namespace: str,
        token: str,
        *,
        ca_file: str | None = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.namespace = namespace
        self.token = token
        self.context = ssl.create_default_context(cafile=ca_file)

    @classmethod
    def from_environment(cls, namespace: str | None = None) -> "KubernetesScaleClient":
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            raise RuntimeError("KUBERNETES_SERVICE_HOST is not set")
        selected_namespace = namespace or (SERVICE_ACCOUNT / "namespace").read_text().strip()
        token = (SERVICE_ACCOUNT / "token").read_text().strip()
        return cls(
            f"https://{host}:{port}",
            selected_namespace,
            token,
            ca_file=str(SERVICE_ACCOUNT / "ca.crt"),
        )

    def _request(self, method: str, path: str, body: Any | None = None) -> dict[str, Any]:
        request = Request(
            f"{self.endpoint}{path}",
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        with urlopen(request, timeout=15, context=self.context) as response:
            return json.loads(response.read())

    def get(self, deployment: str) -> DeploymentScale:
        value = self._request(
            "GET",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{deployment}/scale",
        )
        return DeploymentScale(
            replicas=int(value["spec"]["replicas"]),
            resource_version=str(value["metadata"]["resourceVersion"]),
        )

    def set(self, deployment: str, replicas: int, resource_version: str) -> DeploymentScale:
        if replicas <= 0:
            raise ValueError("deployment replicas must be positive")
        value = self._request(
            "PUT",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{deployment}/scale",
            {
                "apiVersion": "autoscaling/v1",
                "kind": "Scale",
                "metadata": {
                    "name": deployment,
                    "namespace": self.namespace,
                    "resourceVersion": resource_version,
                },
                "spec": {"replicas": replicas},
            },
        )
        return DeploymentScale(
            replicas=int(value["spec"]["replicas"]),
            resource_version=str(value["metadata"]["resourceVersion"]),
        )
