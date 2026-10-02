import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./tests/browser",
  timeout: 30000,
  workers: 1,
  use: {
    baseURL: process.env.FRONTEND_URL ?? "http://localhost:3000",
    headless: true,
  },
  reporter: "list",
});
