import { Absurd, SQLiteDatabase } from "@absurd-sqlite/sdk";
import sqlite from "better-sqlite3";

async function main() {
  const extensionPath = process.env.ABSURD_DATABASE_EXTENSION_PATH;
  const dbPath = process.env.ABSURD_DATABASE_PATH;
  if (!extensionPath || !dbPath) {
    throw new Error(
      "ABSURD_DATABASE_EXTENSION_PATH and ABSURD_DATABASE_PATH must be set",
    );
  }
  const db = sqlite(dbPath) as unknown as SQLiteDatabase;

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
