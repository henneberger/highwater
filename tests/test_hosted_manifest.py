from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RENDER = ROOT / "deploy" / "kubernetes" / "render.py"
DIGEST = "0" * 64


class HostedManifestTest(unittest.TestCase):
    def test_renderer_requires_immutable_images_and_resolves_every_value(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "hosted.yaml"
            subprocess.run([
                sys.executable, str(RENDER),
                "--server-image", f"ghcr.io/highwater/server@sha256:{DIGEST}",
                "--application-image", f"ghcr.io/acme/shopping@sha256:{DIGEST}",
                "--application-module", "shopping_app",
                "--process", "shopping-assistant",
                "--task-queue", "shopping-production",
                "--journal", "s3://highwater-production/journal",
                "--output", str(output),
            ], check=True)
            manifest = output.read_text()
            for placeholder in (
                "IMAGE_TAG",
                "APPLICATION_IMAGE",
                "APPLICATION_PROCESS",
                "APPLICATION_TASK_QUEUE",
                "HIGHWATER_JOURNAL_BUCKET",
            ):
                self.assertNotIn(placeholder, manifest)
            self.assertEqual(manifest.count(f"@sha256:{DIGEST}"), 6)
            self.assertIn('args: ["shopping_app", "--process-only"', manifest)
            self.assertIn("fsGroup: 65532", manifest)
            self.assertIn("readOnlyRootFilesystem: true", manifest)
            self.assertIn("runtimeClassName: gvisor", manifest)
            self.assertEqual(manifest.count('--min-replicas\n            - "1"'), 2)
            self.assertEqual(manifest.count('--process-partitions 1,2,3,4'), 2)
            self.assertEqual(manifest.count('"--process-partitions", "1,2,3,4"'), 2)
            self.assertNotIn('--data-plane-only', manifest)
            self.assertEqual(manifest.count('fieldPath: metadata.uid'), 2)
            self.assertEqual(manifest.count('startupProbe:'), 2)
            self.assertEqual(manifest.count('strategy: {type: Recreate}'), 2)
            self.assertIn('metadata: {name: highwater-public}\nspec:\n  selector: {app: highwater-core}', manifest)

    def test_renderer_rejects_mutable_image_tags(self) -> None:
        result = subprocess.run([
            sys.executable, str(RENDER),
            "--server-image", "ghcr.io/highwater/server:latest",
            "--application-image", f"ghcr.io/acme/app@sha256:{DIGEST}",
            "--application-module", "app",
            "--process", "process",
            "--task-queue", "production",
            "--journal", "s3://highwater-production/journal",
        ], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("immutable @sha256 digest", result.stderr)


if __name__ == "__main__":
    unittest.main()
