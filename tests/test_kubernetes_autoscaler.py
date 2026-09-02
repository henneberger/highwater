from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from highwater.kubernetes import KubernetesScaleClient


class Response:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.value).encode()


class KubernetesAutoscalerTest(unittest.TestCase):
    @patch("highwater.kubernetes.urlopen")
    def test_scales_deployment_to_zero(self, request):
        request.return_value = Response({
            "metadata": {"resourceVersion": "19"},
            "spec": {"replicas": 0},
        })
        client = KubernetesScaleClient("https://kubernetes", "applications", "token")
        updated = client.set("shopping-worker", 0, "18")
        self.assertEqual(updated.replicas, 0)
        body = json.loads(request.call_args.args[0].data)
        self.assertEqual(body["spec"]["replicas"], 0)

    @patch("highwater.kubernetes.urlopen")
    def test_reads_and_conditionally_updates_deployment_scale(self, request):
        request.side_effect = [
            Response({"metadata": {"resourceVersion": "17"}, "spec": {"replicas": 2}}),
            Response({"metadata": {"resourceVersion": "18"}, "spec": {"replicas": 4}}),
        ]
        client = KubernetesScaleClient("https://kubernetes", "applications", "token")
        current = client.get("shopping-worker")
        self.assertEqual(current.replicas, 2)
        updated = client.set("shopping-worker", 4, current.resource_version)
        self.assertEqual(updated.replicas, 4)

        put = request.call_args_list[1].args[0]
        self.assertEqual(put.method, "PUT")
        body = json.loads(put.data)
        self.assertEqual(body["metadata"]["resourceVersion"], "17")
        self.assertEqual(body["spec"]["replicas"], 4)


if __name__ == "__main__":
    unittest.main()
