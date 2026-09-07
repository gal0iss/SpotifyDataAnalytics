import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';


export default defineConfig({
    base: '/SpotifyDataAnalytics/',
  plugins: [react()],
  optimizeDeps: {
    exclude: ['@duckdb/duckdb-wasm'],
  },
});
