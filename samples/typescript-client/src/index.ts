import { homedir } from "node:os";
import { join } from "node:path";
import { Absurd } from "@absurd-sqlite/sdk";

async function main() {
  const extensionPath = "../../target/release/libabsurd.dylib";
  const bundleId = "ing.isbuild.absurd-sqlite-standalone";
  const appLocalDir =
    process.env.APP_LOCAL_DIR ??
    (process.platform === "darwin"
      ? join(homedir(), "Library", "Application Support", bundleId)
      : process.platform === "win32"
        ? join(
            process.env.LOCALAPPDATA ?? join(homedir(), "AppData", "Local"),
            bundleId,
          )
        : join(homedir(), ".local", "share", bundleId));
  const dbPath = join(appLocalDir, "absurd-sqlite.db");

  const absurd = new Absurd(extensionPath, dbPath);

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
    }
  );

  setInterval(async () => {
    console.log("creating queue");
    await absurd.createQueue("default");

    console.log("spawning tasks");

    const spawnedTask = await absurd.spawn(
      "hello",
      { name: "Alice" },
      { queue: "default" }
    );
    console.log("Spawned task ID:", spawnedTask.taskID);
  }, 3000);

  await absurd.startWorker({
    concurrency: 2,
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
