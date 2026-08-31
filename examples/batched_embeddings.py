from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass

from temporal_code import Client, streaming


@dataclass(frozen=True)
class Document:
    document_id: str
    text: str


def local_embedding(text: str, dimensions: int = 8) -> list[float]:
    digest = hashlib.sha256(text.encode()).digest()
    return [round((value - 127.5) / 127.5, 6) for value in digest[:dimensions]]


@streaming.process(key="document_id", build_id="batched-embeddings-v1")
class BatchedEmbeddings:
    @streaming.batch(max_size=128, max_delay=0.025)
    async def embed(self, documents: list[Document]):
        batch_size = len(documents)
        return [
            {
                "document_id": document.document_id,
                "embedding": local_embedding(document.text),
                "batch_size": batch_size,
            }
            for document in documents
        ]


async def main(target: str) -> None:
    client = Client(target, poll_interval=0.005)
    process_id = f"embeddings-{uuid.uuid4().hex[:8]}"
    embeddings = await client.start(BatchedEmbeddings, process_id=process_id)

    await embeddings.send_many([
        Document("doc-1", "Temporal-style durable streaming"),
        Document("doc-2", "event-time watermarks and checkpoints"),
        Document("doc-3", "vector search recommendation"),
    ])
    await asyncio.sleep(0.05)
    await embeddings.send(Document("doc-4", "low traffic still flushes"))
    await embeddings.finish(timeout=10)

    print(json.dumps({
        "process": await embeddings.info(),
        "embeddings": await client.read_operator_changes(process_id),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://127.0.0.1:7233")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.target))
