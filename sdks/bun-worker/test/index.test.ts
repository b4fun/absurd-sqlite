import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "bun:test";
import { Absurd } from "@absurd-sqlite/sdk";

import { BunSqliteConnection } from "../src/sqlite";
import { loadExtension } from "./setup";
let tempDir: string | null = null;

function createDatabaseWithMigrations(): Database {
  tempDir = mkdtempSync(join(tmpdir(), "absurd-sqlite-"));
  const dbPath = join(tempDir, "absurd.db");
  const db = new Database(dbPath);
  loadExtension(db);
  db.query("select absurd_apply_migrations()").get();
  return db;
}

afterEach(() => {
  if (tempDir) {
    rmSync(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

describe("Absurd", () => {
  it("creates and lists queues using the sqlite extension", async () => {
    const db = createDatabaseWithMigrations();
    const conn = new BunSqliteConnection(db);
    const absurd = new Absurd(conn);

    await absurd.createQueue("alpha");
    await absurd.createQueue("beta");

    const queues = await absurd.listQueues();
    expect(queues).toContain("alpha");
    expect(queues).toContain("beta");

    await absurd.dropQueue("alpha");
    const remaining = await absurd.listQueues();
    expect(remaining).not.toContain("alpha");

    const { count } = db
      .query("select count(*) as count from absurd_queues")
      .get() as { count: number };
    expect(count).toBe(1);
    db.close();
  });

  it("closes workers without affecting the sqlite database", async () => {
    const db = createDatabaseWithMigrations();
    const conn = new BunSqliteConnection(db);
    const absurd = new Absurd(conn);

    await absurd.close();

    const { ok } = db.query("select 1 as ok").get() as { ok: number };
    expect(ok).toBe(1);
    db.close();
  });
});
