from __future__ import annotations

from temporal_code import (
    ChildWorkflowOptions,
    continue_as_new,
    execute_child_workflow,
    get_version,
    info,
    now,
    sleep,
    workflow,
)


@workflow.defn
class PageWorkflow:
    @workflow.run
    async def run(self, page: list[int]) -> dict:
        await sleep(0.01)
        return {
            "sum": sum(page),
            "processed_at": now().isoformat(),
            "workflow_id": info()["workflow_id"],
        }


@workflow.defn
class PagedAggregationWorkflow:
    @workflow.run
    async def run(
        self,
        pages: list[list[int]],
        page_index: int = 0,
        total: int = 0,
    ) -> dict:
        algorithm_version = await get_version("aggregation-shape", 1, 2)
        child = await execute_child_workflow(
            PageWorkflow,
            pages[page_index],
            options=ChildWorkflowOptions(parent_close_policy="REQUEST_CANCEL"),
        )
        total += child["sum"]

        if page_index + 1 < len(pages):
            await continue_as_new(pages, page_index + 1, total)

        return {
            "total": total,
            "algorithm_version": algorithm_version,
            "last_child": child,
        }
