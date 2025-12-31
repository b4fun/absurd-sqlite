import assert from "node:assert/strict";
import { join } from "node:path";

import { Absurd, Database, DenoSqliteDatabase } from "../mod.ts";
import { applyMigrations, resolveExtensionPath } from "./setup.ts";

let tempDir: string | null = null;

async function createDatabaseWithMigrations(): Promise<string> {
  tempDir = await Deno.makeTempDir({ prefix: "absurd-sqlite-" });
  const dbPath = join(tempDir, "absurd.db");
  const db = new DenoSqliteDatabase(
    new Database(dbPath, { enableLoadExtension: true }),
  );
  db.loadExtension(resolveExtensionPath());
  applyMigrations(db);
  db.close();
  return dbPath;
}

async function cleanupTempDir(): Promise<void> {
  if (tempDir) {
    await Deno.remove(tempDir, { recursive: true });
    tempDir = null;
  }
}

Deno.test("index: creates and lists queues using the sqlite extension", async () => {
  const dbPath = await createDatabaseWithMigrations();
  const db = new DenoSqliteDatabase(
    new Database(dbPath, { enableLoadExtension: true }),
  );
  const absurd = new Absurd(db, resolveExtensionPath());

  await absurd.createQueue("alpha");
  await absurd.createQueue("beta");

  const queues = await absurd.listQueues();
  assert(queues.includes("alpha"));
  assert(queues.includes("beta"));

  await absurd.dropQueue("alpha");
  const remaining = await absurd.listQueues();
  assert.equal(remaining.includes("alpha"), false);

  const db2 = new Database(dbPath);
  const row = db2.prepare("select count(*) as count from absurd_queues")
    .get() as {
      count: number;
    };
  assert.equal(row.count, 1);
  db2.close();

  await absurd.close();
  await cleanupTempDir();
});

Deno.test("index: closes the sqlite database on close()", async () => {
  const dbPath = await createDatabaseWithMigrations();
  const db = new DenoSqliteDatabase(
    new Database(dbPath, { enableLoadExtension: true }),
  );
  const absurd = new Absurd(db, resolveExtensionPath());

  await absurd.close();

  const db2 = new Database(dbPath);
  const row = db2.prepare("select 1 as ok").get() as { ok: number };
  assert.equal(row.ok, 1);
  db2.close();

  await cleanupTempDir();
});
