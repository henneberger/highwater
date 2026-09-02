/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'index',
    'quickstart',
    {
      type: 'category',
      label: 'Evaluate',
      collapsed: true,
      link: { type: 'doc', id: 'evaluate/index' },
      items: [
        'evaluate/durable-streaming',
        'evaluate/use-cases',
      ],
    },
    {
      type: 'category',
      label: 'Develop',
      collapsed: false,
      link: { type: 'doc', id: 'develop/index' },
      items: [
        {
          type: 'category',
          label: 'Python SDK',
          collapsed: true,
          link: { type: 'doc', id: 'develop/python/index' },
          items: [
            'develop/python/setup',
            'develop/python/processes',
            'develop/python/ingestion',
            'develop/python/batching',
            'develop/python/event-time',
            'develop/python/operators',
            'develop/python/upgrades',
          ],
        },
        'develop/deployments',
        'develop/examples',
      ],
    },
    {
      type: 'category',
      label: 'Operate Highwater',
      collapsed: true,
      link: { type: 'doc', id: 'production/index' },
      items: [
        'production/durability',
        'production/recovery',
        'production/performance',
        'production/scaling',
        'production/sandboxing',
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      collapsed: true,
      link: { type: 'doc', id: 'concepts/index' },
      items: [
        'concepts/durable-processes',
        'concepts/event-time-and-watermarks',
        'concepts/temporal-joins',
        'concepts/serverless-execution',
        'concepts/leases-and-fencing',
        'concepts/checkpoints',
      ],
    },
    {
      type: 'category',
      label: 'References',
      collapsed: true,
      link: { type: 'doc', id: 'references/index' },
      items: [
        'references/python-api',
        'references/options',
        'references/server-options',
        'references/guarantees',
      ],
    },
    {
      type: 'category',
      label: 'Troubleshooting',
      collapsed: true,
      link: { type: 'doc', id: 'troubleshooting/index' },
      items: [
        'troubleshooting/backpressure',
        'troubleshooting/recovery',
        'troubleshooting/invocation-leases',
      ],
    },
    'glossary',
  ],
};

module.exports = sidebars;
