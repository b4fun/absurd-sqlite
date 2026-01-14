import { Database } from "bun:sqlite";
import { describe, expect, it, jest } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { BunSqliteConnection } from "../src/sqlite";

describe("BunSqliteConnection", () => {
  it("rewrites postgres-style params and absurd schema names", async () => {
    const db = new Database(":memory:");
    const conn = new BunSqliteConnection(db);

    await conn.exec("CREATE TABLE absurd_tasks (id, name)");
    await conn.exec("INSERT INTO absurd.tasks (id, name) VALUES ($1, $2)", [
      1,
      "alpha",
    ]);

    const { rows } = await conn.query<{ id: number; name: string }>(
      "SELECT id, name FROM absurd.tasks WHERE id = $1",
      [1]
    );

    expect(rows).toEqual([{ id: 1, name: "alpha" }]);
    db.close();
  });

  it("returns empty rows for non-reader statements", async () => {
    const db = new Database(":memory:");
    const conn = new BunSqliteConnection(db);

    const { rows } = await conn.query("CREATE TABLE t (id)");
    expect(rows).toEqual([]);

    await conn.exec("INSERT INTO t (id) VALUES ($1)", [1]);
    const { rows: inserted } = await conn.query<{ id: number }>(
      "SELECT id FROM t"
    );
    expect(inserted).toEqual([{ id: 1 }]);
    db.close();
  });

  it("decodes JSON from typeless columns", async () => {
    const db = new Database(":memory:");
    const conn = new BunSqliteConnection(db);

    await conn.exec("CREATE TABLE t (payload)");
    await conn.exec("INSERT INTO t (payload) VALUES ($1)", ['{"a":1}']);

    const { rows } = await conn.query<{ payload: { a: number } }>(
      "SELECT payload FROM t"
    );

    expect(rows[0]?.payload).toEqual({ a: 1 });
    db.close();
  });

  it("decodes JSON from blob columns", async () => {
    const db = new Database(":memory:");
    const conn = new BunSqliteConnection(db);

    await conn.exec("CREATE TABLE t_blob (payload BLOB)");
    await conn.exec("INSERT INTO t_blob (payload) VALUES ($1)", [
      Buffer.from(JSON.stringify({ b: 2 })),
    ]);

    const { rows } = await conn.query<{ payload: { b: number } }>(
      "SELECT payload FROM t_blob"
    );

    expect(rows[0]?.payload).toEqual({ b: 2 });
    db.close();
  });

  it("decodes datetime columns into Date objects", async () => {
    const db = new Database(":memory:");
    const conn = new BunSqliteConnection(db);
    const now = Date.now();

    await conn.exec("CREATE TABLE t_date (created_at DATETIME)");
    await conn.exec("INSERT INTO t_date (created_at) VALUES ($1)", [now]);

    const { rows } = await conn.query<{ created_at: Date }>(
      "SELECT created_at FROM t_date"
    );

    expect(rows[0]?.created_at).toBeInstanceOf(Date);
    expect(rows[0]?.created_at.getTime()).toBe(now);
    db.close();
  });

  it("decodes datetime columns stored as strings into Date objects", async () => {
    const db = new Database(":memory:");
    const conn = new BunSqliteConnection(db);
    const targetTime = new Date("2024-05-01T10:00:00Z");
    const timestamp = targetTime.getTime();

    await conn.exec("CREATE TABLE t_str_date (available_at TEXT)");
    // Insert as string to simulate how SQLite might return datetime columns in some cases
    await conn.exec("INSERT INTO t_str_date (available_at) VALUES ($1)", [
      timestamp.toString(),
    ]);

    const { rows } = await conn.query<{ available_at: Date }>(
      "SELECT available_at FROM t_str_date"
    );

    expect(rows[0]?.available_at).toBeInstanceOf(Date);
    expect(rows[0]?.available_at.getTime()).toBe(timestamp);
    db.close();
  });

  it("retries when SQLite reports the database is busy", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "absurd-sqlite-busy-"));
    const dbPath = join(tempDir, "busy.db");
    const primary = new Database(dbPath);
    primary.run("PRAGMA busy_timeout = 1");
    const conn = new BunSqliteConnection(primary);
    await conn.exec("CREATE TABLE t_busy (id INTEGER PRIMARY KEY, value TEXT)");

    const blocker = new Database(dbPath);
    blocker.run("PRAGMA busy_timeout = 1");
    blocker.run("BEGIN EXCLUSIVE");

    let released = false;
    const releaseLock = () => {
      if (released) return;
      released = true;
      try {
        blocker.run("COMMIT");
      } catch {
        // ignore if already closed
      }
      blocker.close();
    };
    const timer = setTimeout(releaseLock, 20);

    try {
      await conn.exec("INSERT INTO t_busy (value) VALUES ($1)", ["alpha"]);
      const { rows } = await conn.query<{ value: string }>(
        "SELECT value FROM t_busy"
      );
      expect(rows[0]?.value).toBe("alpha");
    } finally {
      clearTimeout(timer);
      releaseLock();
      primary.close();
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("retries on locked error codes from SQLite", async () => {
    const lockedError = new Error("SQLITE_LOCKED: mock lock") as any;
    lockedError.code = "SQLITE_LOCKED_SHAREDCACHE";
    lockedError.errno = 6;

    let attempts = 0;
    const statement = {
      all: jest.fn(),
      run: jest.fn(() => {
        attempts++;
        if (attempts === 1) {
          throw lockedError;
        }
        return 1;
      }),
    };

    const querySpy = jest.fn().mockReturnValue(statement as any);
    const db = { query: querySpy } as unknown as Database;
    const conn = new BunSqliteConnection(db);

    await expect(
      conn.exec("UPDATE locked_table SET value = $1 WHERE id = $2", [1, 1])
    ).resolves.toBeUndefined();
    expect(statement.run).toHaveBeenCalledTimes(2);
    expect(querySpy).toHaveBeenCalledTimes(1);
  });
});
