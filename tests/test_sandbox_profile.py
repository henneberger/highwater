from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SandboxProfileTest(unittest.TestCase):
    def test_worker_profile_has_enforced_isolation_boundaries(self) -> None:
        profile = (ROOT / "deploy/sandbox/worker.yaml").read_text()
        required = (
            "runtimeClassName: gvisor",
            "automountServiceAccountToken: false",
            "runAsNonRoot: true",
            "allowPrivilegeEscalation: false",
            "readOnlyRootFilesystem: true",
            'drop: ["ALL"]',
            "seccompProfile:",
            "limits:",
            "memory: \"2Gi\"",
            'policyTypes: ["Ingress", "Egress"]',
            "HIGHWATER_EXECUTION_TOKEN",
            "http://highwater-core:7234",
        )
        for boundary in required:
            self.assertIn(boundary, profile)
        self.assertIn("ingress: []", profile)
        self.assertNotIn("privileged: true", profile)
        self.assertNotIn("hostNetwork: true", profile)
        self.assertNotIn("hostPath:", profile)


if __name__ == "__main__":
    unittest.main()
