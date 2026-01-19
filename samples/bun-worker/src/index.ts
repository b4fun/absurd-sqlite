import run from "@absurd-sqlite/bun-worker";
import { Database } from "bun:sqlite";
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";

configureBunSqlite();

await run(async (absurd) => {
  await absurd.createQueue("default");

  absurd.registerTask(
    {
      name: "hello",
    },
    async (params, ctx) => {
      await ctx.step("init", async () => {
        console.log("init step");
        ctx.emitEvent("progress", { message: "Initialization complete" });
        return {};
      });

      await ctx.sleepFor("back off 15s", 15);

      await ctx.step("process", async () => {
        console.log("process step");
        ctx.emitEvent("progress", { message: "Processing complete" });
        return {};
      });

      const name = params.name || "world";

      console.log(`Saying hello to ${name}`);
      return { greeting: `Hello, ${name}!` };
    }
  );

  setInterval(async () => {
    console.log("spawning tasks");

    const spawnedTask = await absurd.spawn(
      "hello",
      { name: "Alice" },
      { queue: "default" }
    );
    console.log("Spawned task ID:", spawnedTask.taskID);
  }, 3000);
});

function configureBunSqlite(): void {
  if (process.platform !== "darwin") {
    return;
  }

  const customSQLite = resolveCustomSQLitePath();
  if (!customSQLite) {
    throw new Error(
      "Bun's SQLite build on macOS does not support extensions. " +
        "Install sqlite via Homebrew and set ABSURD_SQLITE_CUSTOM_SQLITE_PATH " +
        "to the libsqlite3.dylib path."
    );
  }
  Database.setCustomSQLite(customSQLite);
}

function resolveCustomSQLitePath(): string | null {
  const envPath = process.env.ABSURD_SQLITE_CUSTOM_SQLITE_PATH;
  if (envPath && existsSync(envPath)) {
    return envPath;
  }

  const prefixes = [
    process.env.HOMEBREW_PREFIX,
    "/opt/homebrew",
    "/usr/local",
  ].filter(Boolean) as string[];

  for (const prefix of prefixes) {
    const optPath = join(prefix, "opt", "sqlite", "lib", "libsqlite3.dylib");
    if (existsSync(optPath)) {
      return optPath;
    }

    const cellarPath = join(prefix, "Cellar", "sqlite");
    if (!existsSync(cellarPath)) {
      continue;
    }
    try {
      const entries = readdirSync(cellarPath, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory()) {
          continue;
        }
        const candidate = join(
          cellarPath,
          entry.name,
          "lib",
          "libsqlite3.dylib"
        );
        if (existsSync(candidate)) {
          return candidate;
        }
      }
    } catch {
      // fall through to other prefixes
    }
  }

  return null;
}
