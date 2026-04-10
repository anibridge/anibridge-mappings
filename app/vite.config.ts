import { cloudflare } from "@cloudflare/vite-plugin";
import { defineConfig, type Plugin } from "vite";
import tailwindcss from "@tailwindcss/vite";
import { createReadStream, existsSync } from "node:fs";
import { resolve } from "node:path";

/** Serve files from ../data/out/ in dev mode so the worker doesn't need filesystem access. */
const serveLocalData = (): Plugin => ({
  name: "serve-local-data",
  configureServer(server) {
    const dataDir = resolve(__dirname, "../data/out");
    server.middlewares.use("/data/", (req, res, next) => {
      const fileName = req.url?.split("?")[0]?.replace(/^\//, "") ?? "";
      if (!fileName || fileName.includes("..")) return next();
      const filePath = resolve(dataDir, fileName);
      if (!filePath.startsWith(dataDir) || !existsSync(filePath)) return next();
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Cache-Control", "no-store");
      createReadStream(filePath).pipe(res);
    });
  },
});

export default defineConfig({
  plugins: [serveLocalData(), cloudflare(), tailwindcss()],
  server: { fs: { allow: [".."] } },
});
