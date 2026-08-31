// @ts-check

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Highwater Documentation',
  tagline: 'Durable execution for streaming applications',
  favicon: 'img/mark.svg',
  url: 'https://highwater.cloud',
  baseUrl: '/docs/',
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  organizationName: 'henneberger',
  projectName: 'highwater',
  markdown: { mermaid: true },
  themes: ['@docusaurus/theme-mermaid'],
  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          showLastUpdateAuthor: false,
          showLastUpdateTime: false,
        },
        blog: false,
        theme: { customCss: require.resolve('./src/css/custom.css') },
      },
    ],
  ],
  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Highwater Docs',
      logo: { alt: 'Highwater mark', src: 'img/mark.svg' },
      items: [
        { label: 'Quickstart', to: '/quickstart', position: 'left' },
        { label: 'Develop', to: '/develop', position: 'left' },
        { label: 'Operate', to: '/production', position: 'left' },
        { label: 'Concepts', to: '/concepts', position: 'left' },
        { label: 'References', to: '/references', position: 'left' },
        { label: 'GitHub', href: 'https://github.com/henneberger/highwater', position: 'right' },
      ],
    },
    docs: {
      sidebar: { hideable: true, autoCollapseCategories: true },
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Build',
          items: [
            { label: 'Quickstart', to: '/quickstart' },
            { label: 'Python SDK', to: '/develop/python' },
            { label: 'Examples', to: '/develop/examples' },
          ],
        },
        {
          title: 'Operate',
          items: [
            { label: 'Durability', to: '/production/durability' },
            { label: 'Recovery', to: '/production/recovery' },
            { label: 'Scaling', to: '/production/scaling' },
          ],
        },
        {
          title: 'Reference',
          items: [
            { label: 'Python API', to: '/references/python-api' },
            { label: 'CLI', to: '/references/server-options' },
            { label: 'Glossary', to: '/glossary' },
          ],
        },
      ],
      copyright: `Highwater, ${new Date().getFullYear()}.`,
    },
    prism: {
      additionalLanguages: ['bash', 'toml'],
    },
  },
};

module.exports = config;
