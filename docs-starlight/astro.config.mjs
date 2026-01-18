// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightThemeRapide from 'starlight-theme-rapide'

import d2 from 'astro-d2';

// https://astro.build/config
export default defineConfig({
    integrations: [starlight({
        plugins: [starlightThemeRapide()],
        title: 'Absurd SQLite',
        social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/b4fun/absurd-sqlite' }],
        sidebar: [
            {
                label: 'Guides',
                items: [
                    // Each item here is one entry in the navigation menu.
                    { label: 'Example Guide', slug: 'guides/example' },
                ],
            },
    {
      label: 'Concepts',
      items: [
        { label: 'Durable Workflow', slug: 'concepts/durable-workflow' },
        { label: 'Absurd SQLite', slug: 'concepts/absurd-sqlite' },
      ],
    },
            {
                label: 'Reference',
                autogenerate: { directory: 'reference' },
            },
            {
                label: 'SDKs',
      collapsed: true,
                autogenerate: { directory: 'sdks' },
            },
        ],
		}), d2()],
});