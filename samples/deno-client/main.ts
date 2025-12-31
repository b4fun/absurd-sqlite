import { homedir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { Absurd, openDenoDatabase } from "../../sdks/deno/mod.ts";

function existsSync(path: string): boolean {
  try {
    Deno.statSync(path);
    return true;
  } catch {
    return false;
  }
}

function resolveExtensionPath(): string {
  const sampleDir = fileURLToPath(new URL(".", import.meta.url));
  const repoRoot = join(sampleDir, "..", "..");
  const extensionBase = join(repoRoot, "target", "release", "libabsurd");
  const platformExt = Deno.build.os === "windows"
    ? ".dll"
    : Deno.build.os === "darwin"
      ? ".dylib"
      : ".so";
  const candidates = [extensionBase, `${extensionBase}${platformExt}`];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  throw new Error(
    `SQLite extension not found at ${extensionBase} (expected ${platformExt})`,
  );
}

async function main() {
  const extensionPath = resolveExtensionPath();
  const bundleId = "ing.isbuild.absurd-sqlite-standalone";
  const appLocalDir = Deno.env.get("APP_LOCAL_DIR") ??
    (Deno.build.os === "darwin"
      ? join(homedir(), "Library", "Application Support", bundleId)
      : Deno.build.os === "windows"
        ? join(
          Deno.env.get("LOCALAPPDATA") ??
          join(homedir(), "AppData", "Local"),
          bundleId,
        )
        : join(homedir(), ".local", "share", bundleId));

  await Deno.mkdir(appLocalDir, { recursive: true });

  const dbPath = join(appLocalDir, "absurd-sqlite.db");
  const db = openDenoDatabase(dbPath);
  const absurd = new Absurd(db, extensionPath);

  absurd.registerTask(
    {
      name: "hello",
    },
    async (params, ctx) => {
      await ctx.step("init", async () => {
        console.log("init step");
        ctx.emitEvent("progress", { message: "Initialization complete" });
      });

      await ctx.sleepFor("back off 15s", 15);

      await ctx.step("process", async () => {
        console.log("process step");
        ctx.emitEvent("progress", { message: "Processing complete" });
      });

      const name = params.name || "world";

      console.log(`Saying hello to ${name}`);
      return { greeting: `Hello, ${name}!` };
    },
  );

  setInterval(async () => {
    console.log("creating queue");
    await absurd.createQueue("default");

    console.log("spawning tasks");

    const spawnedTask = await absurd.spawn(
      "hello",
      { name: "Alice" },
      { queue: "default" },
    );
    console.log("Spawned task ID:", spawnedTask.taskID);
  }, 3000);

  await absurd.startWorker({
    concurrency: 2,
  });
}

main().catch((err) => {
  console.error(err);
  Deno.exit(1);
});
