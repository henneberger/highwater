from __future__ import annotations

import asyncio

from temporal_code import (
    Client,
    WorkflowCancelled,
    WorkflowFailed,
    WorkflowOptions,
    wait_condition,
    workflow,
)


@workflow.defn
class LongRunningWorkflow:
    def __init__(self) -> None:
        self.finished = False

    @workflow.run
    async def run(self, label: str) -> str:
        await wait_condition(lambda: self.finished)
        return label

    @workflow.signal
    def finish(self) -> None:
        self.finished = True

    @workflow.query
    def state(self) -> dict:
        return {"finished": self.finished}


async def main(target: str = "http://127.0.0.1:7233") -> None:
    client = Client(target)
    options = WorkflowOptions(task_queue="examples", execution_timeout=30)

    completed = await client.start_workflow(
        LongRunningWorkflow,
        "completed",
        workflow_id="lifecycle-completed",
        options=options,
    )
    print(await completed.query("state"))
    await completed.signal("finish")
    print(await completed.result())

    cancelled = await client.start_workflow(
        LongRunningWorkflow,
        "cancelled",
        workflow_id="lifecycle-cancelled",
        options=options,
    )
    await cancelled.cancel()
    try:
        await cancelled.result()
    except WorkflowCancelled as error:
        print(error)

    terminated = await client.start_workflow(
        LongRunningWorkflow,
        "terminated",
        workflow_id="lifecycle-terminated",
        options=options,
    )
    await terminated.terminate("operator request")
    try:
        await terminated.result()
    except WorkflowFailed as error:
        print(error)

    event_types = [event.type for event in await completed.history()]
    print(event_types)


if __name__ == "__main__":
    asyncio.run(main())
