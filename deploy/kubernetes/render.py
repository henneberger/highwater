from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "hosted.yaml"
IMAGE = re.compile(r"^[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$")
DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
PYTHON_MODULE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)
S3_URI = re.compile(r"^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](?:/[^\s]*)?$", re.ASCII)


def immutable_image(value: str) -> str:
    if not IMAGE.fullmatch(value):
        raise argparse.ArgumentTypeError(
            "images must use a lowercase registry path and an immutable @sha256 digest"
        )
    return value


def dns_label(value: str) -> str:
    if not DNS_LABEL.fullmatch(value):
        raise argparse.ArgumentTypeError("value must be a Kubernetes DNS label")
    return value


def python_module(value: str) -> str:
    if not PYTHON_MODULE.fullmatch(value):
        raise argparse.ArgumentTypeError("value must be an importable Python module name")
    return value


def journal_uri(value: str) -> str:
    if not S3_URI.fullmatch(value):
        raise argparse.ArgumentTypeError("journal URI must be an s3:// bucket and prefix")
    return value.rstrip("/")


def render(arguments: argparse.Namespace) -> str:
    document = SOURCE.read_text()
    replacements = {
        "ghcr.io/henneberger/highwater-server:IMAGE_TAG": arguments.server_image,
        "APPLICATION_IMAGE": arguments.application_image,
        "APPLICATION_PROCESS": arguments.process,
        "APPLICATION_TASK_QUEUE": arguments.task_queue,
        "s3://HIGHWATER_JOURNAL_BUCKET/production": arguments.journal,
        'args: ["app", "--process-only"': (
            f'args: ["{arguments.application_module}", "--process-only"'
        ),
    }
    for source, target in replacements.items():
        occurrences = document.count(source)
        if occurrences == 0:
            raise RuntimeError(f"deployment template no longer contains {source!r}")
        document = document.replace(source, target)
    unresolved = sorted(set(re.findall(
        r"(?:IMAGE_TAG|APPLICATION_[A-Z_]+|HIGHWATER_JOURNAL_BUCKET)", document,
    )))
    if unresolved:
        raise RuntimeError(f"unresolved deployment values: {', '.join(unresolved)}")
    return document


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Render an immutable Highwater production Kubernetes manifest.",
    )
    value.add_argument("--server-image", required=True, type=immutable_image)
    value.add_argument("--application-image", required=True, type=immutable_image)
    value.add_argument("--application-module", required=True, type=python_module)
    value.add_argument("--process", required=True, type=dns_label)
    value.add_argument("--task-queue", required=True, type=dns_label)
    value.add_argument("--journal", required=True, type=journal_uri)
    value.add_argument("--output", type=Path)
    return value


def main() -> None:
    arguments = parser().parse_args()
    document = render(arguments)
    if arguments.output:
        arguments.output.write_text(document)
    else:
        sys.stdout.write(document)


if __name__ == "__main__":
    main()
