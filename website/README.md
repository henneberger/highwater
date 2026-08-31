# Highwater documentation website

The site uses Docusaurus 3 and serves MDX documents from `docs/` at root-level routes.

## Run locally

```bash
yarn install
yarn start
```

## Build the site

```bash
yarn build
yarn serve
```

The production build fails on broken internal links, broken anchors, invalid MDX, or missing sidebar documents.

## Content structure

- `docs/evaluate`: product fit and use cases
- `docs/develop`: task-oriented SDK guides
- `docs/production`: deployment, durability, recovery, and scaling
- `docs/concepts`: execution and event-time explanations
- `docs/references`: exhaustive interfaces and guarantees
- `docs/troubleshooting`: symptom-oriented operational help

Use sentence-case headings, short paragraphs, second-person instructions, and concrete claims. Keep customer tasks in Develop or Operate and place deeper explanations in Concepts. Do not expose execution-language, broker, storage-engine, or partition-tuning details in the customer path.
