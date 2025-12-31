import sqlite from "better-sqlite3";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

import { Absurd, SQLiteDatabase } from "../src/index";

const testDir = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = join(testDir, "../../..");
const extensionBase = join(repoRoot, "target/release/libabsurd");

function resolveExtensionPath(base: string): string {
  const platformExt =
    process.platform === "win32"
      ? ".dll"
      : process.platform === "darwin"
      ? ".dylib"
      : ".so";
  const candidates = [base, `${base}${platformExt}`];
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  throw new Error(
    `SQLite extension not found at ${base} (expected ${platformExt})`
  );
}

const extensionPath = resolveExtensionPath(extensionBase);

let tempDir: string | null = null;

function createDatabaseWithMigrations(): string {
  tempDir = mkdtempSync(join(tmpdir(), "absurd-sqlite-"));
  const dbPath = join(tempDir, "absurd.db");
  const db = new sqlite(dbPath);
  db.loadExtension(extensionPath);
  db.prepare("select absurd_apply_migrations()").get();
  db.close();
  return dbPath;
}

afterEach(() => {
  if (tempDir) {
    rmSync(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

describe("Absurd", () => {
  it("creates and lists queues using the sqlite extension", async () => {
    const dbPath = createDatabaseWithMigrations();
    const db = new sqlite(dbPath) as unknown as SQLiteDatabase;
    const absurd = new Absurd(db, extensionPath);

    await absurd.createQueue("alpha");
    await absurd.createQueue("beta");

    const queues = await absurd.listQueues();
    expect(queues).toContain("alpha");
    expect(queues).toContain("beta");

    await absurd.dropQueue("alpha");
    const remaining = await absurd.listQueues();
    expect(remaining).not.toContain("alpha");

    const db2 = new sqlite(dbPath);
    const { count } = db2
      .prepare("select count(*) as count from absurd_queues")
      .get() as { count: number };
    expect(count).toBe(1);
    db.close();

    await absurd.close();
  });

  it("closes the sqlite database on close()", async () => {
    const dbPath = createDatabaseWithMigrations();
    const db = new sqlite(dbPath) as unknown as SQLiteDatabase;
    const absurd = new Absurd(db, extensionPath);

    await absurd.close();

    const db2 = new sqlite(dbPath);
    const { ok } = db2.prepare("select 1 as ok").get() as { ok: number };
    expect(ok).toBe(1);
    db.close();
  });
});
