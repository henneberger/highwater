"""A realistic, multi-stage AI support topology expressed with Highwater specs."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from highwater import (
    Comparison,
    StreamOptions,
    TemporalJoinType,
    WatermarkMode,
    WindowAggregation,
    streaming,
    workflow,
)
from highwater.dag import Dag


knowledge = streaming.versioned("support-knowledge-versions", key="topic")
account_policy = streaming.versioned("account-policy-versions", key="customer_id")


class SupportModel:
    async def answer(
        self,
        *,
        message: str,
        article: dict | None,
        policy: dict | None,
        recent_topics: list[str],
    ) -> dict:
        await asyncio.sleep(0.001)
        confidence = 0.91 if article is not None else 0.42
        return {
            "answer": f"Suggested response for: {message}",
            "confidence": confidence,
            "requires_human": confidence < 0.7 or policy is None,
            "priority": 3 if policy and policy.get("priority_support") else 1,
            "tokens": 180 + 20 * len(recent_topics),
        }


support_model = SupportModel()


@streaming.process(
    key="customer_id",
    event_time="occurred_at",
    state_version=2,
    build_id="support-triage-v2",
)
@dataclass
class SupportTriage:
    recent_topics: list[str] = field(default_factory=list)
    messages_seen: int = 0

    @streaming.migrate(from_version=1)
    def migrate_v1(self, state: dict) -> dict:
        return {
            "recent_topics": state.get("recent_topics", []),
            "messages_seen": 0,
        }

    @streaming.event
    async def decide(self, message, context):
        article = await knowledge.get(message.topic, as_of=context.event_time)
        policy = await account_policy.get(
            message.customer_id,
            as_of=context.event_time,
        )
        result = await support_model.answer(
            message=message.text,
            article=article,
            policy=policy,
            recent_topics=self.recent_topics,
        )
        self.messages_seen += 1
        self.recent_topics = [*self.recent_topics, message.topic][-8:]
        return {
            "case_id": message.case_id,
            "customer_id": message.customer_id,
            "topic": message.topic,
            "answer": result["answer"],
            "confidence": result["confidence"],
            "requires_human": result["requires_human"],
            "priority": result["priority"],
            "tokens": result["tokens"],
            "messages_seen": self.messages_seen,
        }


@streaming.process(key="customer_id", build_id="escalation-coordinator-v1")
@dataclass
class EscalationCoordinator:
    handoffs: int = 0

    @streaming.event
    async def assign(self, routed):
        self.handoffs += 1
        decision = routed.probe.value
        plan = None if routed.version is None else routed.version.value
        return {
            "case_id": decision["case_id"],
            "customer_id": decision["customer_id"],
            "queue": "general" if plan is None else plan["queue"],
            "priority": decision["priority"],
            "handoff_number": self.handoffs,
        }


@workflow.defn
class KeepSupportMessage:
    @workflow.run
    async def run(self, record: dict) -> dict:
        return record["value"]


@workflow.defn
class EscalationDecision:
    @workflow.run
    async def run(self, record: dict) -> dict:
        return record["value"]


@workflow.defn
class RouteAtDecisionTime:
    @workflow.run
    async def run(self, joined: dict) -> dict:
        return {
            "decision": joined["probe"]["value"],
            "service_plan": (
                None
                if joined["version"] is None
                else joined["version"]["value"]
            ),
        }


@workflow.defn
class ResolutionOutcome:
    @workflow.run
    async def run(self, pair: dict) -> dict:
        return {
            "decision": pair["left"]["value"],
            "feedback": pair["right"]["value"],
        }


@workflow.defn
class TokenUsageWindow:
    @workflow.run
    async def run(self, window: dict) -> dict:
        return window


@workflow.defn
class FeedbackScoreWindow:
    @workflow.run
    async def run(self, window: dict) -> dict:
        return window


@workflow.defn
class PageOnCall:
    @workflow.run
    async def run(self, record: dict) -> dict:
        return record["value"]


SOURCE_MANAGED = StreamOptions(watermark_mode=WatermarkMode.SOURCE_MANAGED)


def topology() -> Dag:
    dag = Dag("ai-support-operations")
    for stream in (
        "support-events",
        "unique-support-events",
        "support-knowledge-versions",
        "account-policy-versions",
        "agent-decisions",
        "escalations",
        "service-plan-versions",
        "routed-escalations",
        "human-handoffs",
        "pager-events",
        "customer-feedback",
        "resolution-outcomes",
        "hourly-token-usage",
        "daily-feedback-scores",
    ):
        dag.stream(stream, SOURCE_MANAGED)

    return (
        dag.deduplicate(
            "deduplicate-support-events",
            input="support-events",
            workflow=KeepSupportMessage,
            output="unique-support-events",
        )
        .process(
            SupportTriage,
            input="unique-support-events",
            process_id="support-triage",
            output="agent-decisions",
        )
        .filter(
            "select-escalations",
            input="agent-decisions",
            workflow=EscalationDecision,
            field="requires_human",
            comparison=Comparison.EQUAL,
            operand=True,
            output="escalations",
        )
        .temporal_join(
            "route-escalations",
            probe="escalations",
            versions="service-plan-versions",
            workflow=RouteAtDecisionTime,
            join_type=TemporalJoinType.LEFT,
            output="routed-escalations",
        )
        .process(
            EscalationCoordinator,
            input="routed-escalations",
            process_id="coordinate-handoffs",
            output="human-handoffs",
        )
        .filter(
            "page-high-priority",
            input="human-handoffs",
            workflow=PageOnCall,
            field="priority",
            comparison=Comparison.GREATER_THAN_OR_EQUAL,
            operand=3,
            output="pager-events",
        )
        .window(
            "aggregate-hourly-token-usage",
            input="agent-decisions",
            workflow=TokenUsageWindow,
            size=3600,
            slide=300,
            start_at=0,
            aggregation=WindowAggregation.SUM,
            value="tokens",
            output="hourly-token-usage",
        )
        .interval_join(
            "match-decisions-to-feedback",
            left="agent-decisions",
            right="customer-feedback",
            workflow=ResolutionOutcome,
            lower=0,
            upper=604_800,
            output="resolution-outcomes",
        )
        .window(
            "aggregate-daily-feedback-scores",
            input="customer-feedback",
            workflow=FeedbackScoreWindow,
            size=86_400,
            start_at=0,
            aggregation=WindowAggregation.SUM,
            value="score",
            output="daily-feedback-scores",
        )
    )


if __name__ == "__main__":
    print(topology().snapshot(), end="")
