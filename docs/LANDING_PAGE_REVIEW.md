# Landing-page persona review

## Personas

### Skeptical platform buyer

A staff platform engineer at a large company evaluating Highwater for revenue-critical state. This persona looks for exact failure boundaries, operational evidence, differentiation, and risks hidden by product language.

### Application developer

A senior Python developer building recommendation or device applications. This persona understands events but does not want to learn checkpoint or watermark internals. They evaluate time-to-understanding, API intuition, accessibility, and the next developer action.

### Technical product and brand lead

A developer-infrastructure product lead responsible for positioning, launch credibility, visual hierarchy, conversion, search and social readiness, and consistency with the language guide.

## Consensus

All three reviewers understood the core category quickly: stateful streaming in ordinary Python, with durable per-key state and ordering. They identified the Process model and precise failure contract as the strongest sections.

Their shared concerns were:

- the metaphorical hero required more interpretation than the literal product claim;
- event-time completeness did not say who advances progress;
- the external-effect caveat was too visually quiet;
- unsupported scale language reduced trust;
- mobile navigation and disabled placeholders needed accessibility work;
- a public launch needs working documentation, legal, social, and early-access destinations.

## Changes applied

- Replaced the hero with the style-guide formulation: “Write stateful streaming applications as ordinary Python.”
- Made recovery, per-key ordering, and event time explicit in the hero lead.
- Replaced “millions of unrelated keys” with an unqualified concurrency statement.
- Defined the event-time gate in terms of configured source progress.
- Added semantic screen-reader text to the visual event timeline.
- Increased the retry and external-idempotency note to normal body prominence.
- Removed generic external-API language from benefit copy.
- Tightened batching and call-to-action language.
- Added the early-access action to mobile navigation.
- Made disabled placeholder links unfocusable.
- Added dynamic menu labels, Escape handling, focus return, and a 44-pixel menu target.
- Added social metadata, `robots.txt`, `sitemap.xml`, and deployment security guidance.

## Deliberate decisions

The `pip install highwater` command remains because the intended public package and installation contract were explicitly selected before release. The page is not being published yet, and the primary action remains early access.

The page does not include a feature matrix against Temporal or Flink. Highwater's public position should stand on the Process model, event time, and durable streaming contract. Comparative material belongs in evaluation documentation where the distinctions can be precise.

The documentation and social URLs remain centralized placeholders in `landing/config.js`. A disabled destination is preferable to inventing a public account.

## Required before publication

- Publish the package or reconsider displaying the install command.
- Populate documentation, early-access, GitHub, social, privacy, and terms links.
- Replace the mailto early-access action with a hosted form.
- Add a 1200×630 Open Graph image and image metadata.
- Verify every public guarantee against the deployed durability architecture.
- Configure CloudFront security headers, compression, TLS, and cache policies.
- Run automated accessibility and cross-browser checks against the deployed preview.
