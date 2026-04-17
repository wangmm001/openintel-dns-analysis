import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  outDir: '../../dist/web',
  trailingSlash: 'ignore',
  build: {
    format: 'directory',
    assets: '_assets',
  },
  integrations: [mdx()],
  vite: {
    plugins: [tailwindcss()],
    ssr: {
      noExternal: ['echarts'],
    },
  },
  server: {
    host: '127.0.0.1',
    port: 4321,
  },
});
