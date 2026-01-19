import { Absurd, SQLiteConnection, SQLiteDatabase } from "@absurd-sqlite/sdk";
import sqlite from "better-sqlite3";

async function main() {
  const extensionPath = process.env.ABSURD_DATABASE_EXTENSION_PATH;
  const dbPath = process.env.ABSURD_DATABASE_PATH;
  if (!extensionPath || !dbPath) {
    throw new Error(
      "ABSURD_DATABASE_EXTENSION_PATH and ABSURD_DATABASE_PATH must be set",
    );
  }
  const db = sqlite(dbPath) as SQLiteDatabase;
  db.loadExtension(extensionPath);
  db.prepare("select absurd_apply_migrations()").run();
  const conn = new SQLiteConnection(db, { verbose: console.log });

  const absurd = new Absurd(conn);

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
