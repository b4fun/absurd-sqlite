import { spawnSync } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(scriptDir, "..");
const distDir = resolve(rootDir, "dist");
const tscBinary = resolve(
  rootDir,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "tsc.cmd" : "tsc"
);

rmSync(distDir, { recursive: true, force: true });

run(tscBinary, ["--project", "tsconfig.build.json"]);
run(tscBinary, ["--project", "tsconfig.cjs.json"]);

mkdirSync(resolve(distDir, "cjs"), { recursive: true });
writeFileSync(
  resolve(distDir, "cjs", "package.json"),
  `${JSON.stringify({ type: "commonjs" })}\n`
);

function run(command, args) {
  const result = spawnSync(command, args, {
    cwd: rootDir,
    stdio: "inherit",
  });

  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
