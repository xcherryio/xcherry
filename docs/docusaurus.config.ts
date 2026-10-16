/*
 * Modifications Copyright (c) 2026 Super Durable, Inc.
 *
 * Modifications after the Legacy Cutoff are licensed under the
 * Super Durable Source License 1.0.
 * Legacy Materials remain under their original licenses.
 * See LICENSE and LEGACY_NOTICES.md.
 */

import {fileURLToPath} from 'node:url';
import clientRedirects from '@docusaurus/plugin-client-redirects';
import {themes as prismThemes} from 'prism-react-renderer';
import type {Config, LoadContext} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import redirects from './redirects.json';

const config: Config = {
  title: 'Super Durable Docs',
  tagline: 'Durable Execution for backend engineering',
  // The query makes browsers that cached the Branch D icon at this path fetch the new mark; bump it when the mark changes.
  favicon: 'img/brand/dex-mark.svg?v=pipe-d',

  future: {
    v4: true,
  },

  url: 'https://docs.superdurable.io',
  baseUrl: '/',
  trailingSlash: true,

  organizationName: 'superdurable',
  projectName: 'dex',

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  onBrokenMarkdownLinks: 'throw',
  onDuplicateRoutes: 'throw',

  headTags: [
    {
      tagName: 'meta',
      attributes: {
        name: 'google-site-verification',
        content: 'sZ7KTJpIynUmNq33gAzRLllx2nJvi2bvTpOYlHc7klg',
      },
    },
    {
      tagName: 'script',
      attributes: {},
      innerHTML: `
(function () {
  try {
    var match = document.cookie.match(/(?:^|; )superdurable-theme=(light|dark)(?:;|$)/);
    var cookieTheme = match ? match[1] : null;
    var storedTheme = window.localStorage.getItem('theme');
    var systemTheme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    var theme = cookieTheme || ((storedTheme === 'light' || storedTheme === 'dark') ? storedTheme : systemTheme);
    window.localStorage.setItem('theme', theme);
    document.documentElement.setAttribute('data-theme', theme);
    document.documentElement.style.colorScheme = theme;
    if (!cookieTheme && (storedTheme === 'light' || storedTheme === 'dark')) {
      var shared = location.hostname === 'superdurable.io' || location.hostname.endsWith('.superdurable.io');
      document.cookie = 'superdurable-theme=' + storedTheme + '; Path=/; Max-Age=31536000; SameSite=Lax' +
        (shared ? '; Domain=.superdurable.io; Secure' : '');
    }
  } catch (_) {}
})();
(function () {
  try {
    var stored = window.localStorage.getItem('docs-locale');
    var cookieMatch = document.cookie.match(/(?:^|; )superdurable-docs-locale=(en|zh-Hans)(?:;|$)/);
    var preferred = stored === 'en' || stored === 'zh-Hans' ? stored : (cookieMatch ? cookieMatch[1] : null);
    if (preferred !== 'zh-Hans') return;
    var path = location.pathname;
    if (path === '/zh-Hans' || path === '/zh-Hans/' || path.indexOf('/zh-Hans/') === 0) return;
    var next = path === '/' ? '/zh-Hans/' : '/zh-Hans' + path;
    location.replace(next + location.search + location.hash);
  } catch (_) {}
})();`,
    },
  ],

  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'zh-Hans'],
    localeConfigs: {
      en: {label: 'English', htmlLang: 'en'},
      'zh-Hans': {label: '中文', htmlLang: 'zh-Hans'},
    },
  },

  presets: [
    [
      'classic',
      {
        docs: {
          path: 'content',
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/superdurable/dex/tree/main/docs/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          changefreq: 'daily',
          priority: 0.5,
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    (context: LoadContext) =>
      clientRedirects(context, {
        id: 'default',
        fromExtensions: [],
        toExtensions: [],
        redirects: redirects
          .filter(
            ({from, to}) =>
              redirectPathLocale(context, from) === context.i18n.currentLocale &&
              redirectPathLocale(context, to) === context.i18n.currentLocale,
          )
          .map(({from, to}) => ({
            from: pathWithinCurrentLocale(context, from),
            to: pathWithinCurrentLocale(context, to),
          })),
      }),
    () => ({
      name: 'local-flow-definition-renderer',
      configureWebpack: (_config, isServer, {getJSLoader}) => ({
        resolve: {
          symlinks: false,
          // Dex Web's Flow canvas is compiled from its source and shares the docs' React and libraries.
          alias: {
            '@dex-web-v2': fileURLToPath(new URL('../web/app/v2', import.meta.url)),
            ...Object.fromEntries(
              ['react', 'react-dom', '@xyflow/react', 'elkjs'].map((name) => [
                name,
                fileURLToPath(new URL(`./node_modules/${name}`, import.meta.url)),
              ]),
            ),
          },
        },
        module: {
          rules: [
            {
              ...getJSLoader({isServer}),
              test: /\.[jt]sx?$/,
              include: [
                fileURLToPath(new URL('./node_modules/@superdurable/flow-definition-renderer/src', import.meta.url)),
                fileURLToPath(new URL('../web/app/v2', import.meta.url)),
              ],
            },
          ],
        },
      }),
    }),
  ],

  themeConfig: {
    // A raster, deliberately: social cards will not render an SVG.
    image: 'img/brand/dex-social.png',
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
    navbar: {
      logo: {
        alt: 'Super Durable',
        src: 'img/brand/dex-mark.svg',
        href: 'https://superdurable.io/',
        target: '_self',
      },
      items: [
        {
          href: 'https://superdurable.io/dex',
          label: 'Dex',
          position: 'left',
          target: '_self',
        },
        {
          href: 'https://github.com/superdurable',
          label: 'GitHub',
          position: 'left',
          target: '_blank',
        },
        {
          href: 'https://calendar.google.com/appointments/schedules/AcZssZ0XTgrR4TGKOsS-zcB7tu_xqIaYaM3MQGXraOJccpyUe9LK0Z_FF7ImVSw4g_4UGGfx3ykq81mw',
          label: 'Book a call',
          position: 'right',
          className: 'header-booking-link',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Explore',
          items: [
            {label: 'Dex', href: 'https://superdurable.io/dex'},
          ],
        },
        {
          title: 'Company',
          items: [
            {label: 'Team', href: 'https://superdurable.io/team'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Super Durable, Inc.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['java', 'python', 'go', 'typescript', 'rust', 'bash'],
    },
  } satisfies Preset.ThemeConfig,
};

function redirectPathLocale(context: LoadContext, pathname: string): string {
  return (
    context.i18n.locales.find(
      (locale) =>
        locale !== context.i18n.defaultLocale && pathname.startsWith(`/${locale}/`),
    ) ?? context.i18n.defaultLocale
  );
}

function pathWithinCurrentLocale(context: LoadContext, pathname: string): string {
  if (context.i18n.currentLocale === context.i18n.defaultLocale) {
    return pathname;
  }
  return pathname.slice(context.i18n.currentLocale.length + 1) || '/';
}

export default config;
