# Highwater language style guide

## Positioning

Highwater is durable execution for streaming applications. It lets developers express stateful event processing as ordinary code while the platform owns durable state, ordering, event-time progress, retries, backpressure, and scaling.

Use the shorthand “Temporal for streaming” in conversation or comparison, not as the primary headline. Highwater must stand on its own value.

## Audience

Write first for an experienced application or platform engineer who understands events but does not want to operate a stream-processing stack. Assume familiarity with Python, APIs, deployments, and production failures. Do not assume familiarity with checkpoint barriers, storage engines, brokers, or distributed-systems papers.

## Voice

- Direct, calm, and technically credible.
- Product-led rather than research-led.
- Specific about the application outcome.
- Confident without claiming guarantees the implementation does not provide.
- Brief enough to scan, concrete enough to evaluate.

## Message order

1. State what the developer can build.
2. Show the programming model.
3. Explain what Highwater takes responsibility for.
4. Establish the durability and event-time contract.
5. Offer a clear next action.

Lead with “write streaming applications as code,” not with architecture.

## Preferred terms

| Use | Avoid in customer copy |
| --- | --- |
| Process | job graph, topology, task |
| event ingestion | broker, Kafka topic |
| execution runtime | Rust service, language worker |
| durable state | RocksDB, state backend |
| durable history | WAL, log shard |
| partition | key group, lane |
| invocation | worker activation |
| event-time gate | watermark arithmetic |
| deployment | binary, container bundle |
| committed output | sink record |

Implementation documentation may use internal terms when precision requires them.

## Claims

Make claims at the exact boundary Highwater controls:

- “Accepted events survive execution failures.”
- “State and Highwater output commit together.”
- “Invocations may retry; fenced completions prevent stale attempts from committing.”
- “Stable event IDs make uncertain ingestion retries safe.”

Do not say:

- “exactly once” without naming the boundary;
- “serverless” as a synonym for infinitely scalable;
- “zero operations”;
- “never loses data” without defining admission;
- “production-ready” or “enterprise-grade” as unsupported adjectives.

Use measured performance numbers only with the durable boundary, workload, and environment attached.

## Sentence and heading style

- Use sentence-case headings.
- Prefer active voice and verbs: write, send, wait, deploy, recover.
- Keep paragraphs to two or three sentences.
- Use contractions sparingly.
- Avoid rhetorical questions in core product copy.
- Avoid “simply,” “easy,” “magic,” “revolutionary,” and “next-generation.”
- Expand an acronym on first use unless it is part of code.

## Product vocabulary examples

Preferred:

> Write stateful streaming applications as ordinary Python. Highwater keeps each key ordered and recovers accepted events, state, and output after failures.

Avoid:

> Our revolutionary Rust-powered distributed engine combines Temporal, Flink, and differential dataflow with a next-generation object-store WAL.

Preferred:

> Wait until an event timestamp is complete, then run normal application code.

Avoid:

> Configure a source-managed watermark and bind it to a multi-input frontier.

## Calls to action

Use one primary action per surface:

- “Get early access” before public availability.
- “Start building” when installation is real.
- “Read the docs” as the secondary action.

Do not use “Learn more” when a specific destination can be named.

## Social copy

Describe one product outcome per post. Pair code with the guarantee it relies on. Avoid announcing internal milestones as customer value unless they change throughput, latency, recovery, or usability.
