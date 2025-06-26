import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'MonoDB',
  tagline: 'A modern, multi-paradigm database engine written in Rust',
  favicon: '/img/favicon.ico',

  // Future flags
  future: {
    v4: true,
  },

  url: 'https://jacob-walton.github.io',
  baseUrl: '/Mono-DB/',

  organizationName: 'Jacob-Walton',
  projectName: 'Mono-DB',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/Jacob-Walton/Mono-DB/tree/main/monodb-docs/',
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          editUrl: 'https://github.com/Jacob-Walton/Mono-DB/tree/main/monodb-docs/',
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/monodb-social-card.png',
    navbar: {
      title: 'MonoDB',
      logo: {
        alt: 'MonoDB Logo',
        src: '/img/logo-transparent.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/blog',
          label: 'Blog',
          position: 'left'
        },
        {
          href: 'https://github.com/Jacob-Walton/Mono-DB',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/getting-started',
            },
            {
              label: 'NSQL Reference',
              to: '/docs/nsql-reference',
            },
            {
              label: 'API Reference',
              to: '/docs/api/network-protocol',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub Discussions',
              href: 'https://github.com/Jacob-Walton/Mono-DB/discussions',
            },
            {
              label: 'Issues',
              href: 'https://github.com/Jacob-Walton/Mono-DB/issues',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Blog',
              to: '/blog',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/Jacob-Walton/Mono-DB',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Jacob Walton. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'toml', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
