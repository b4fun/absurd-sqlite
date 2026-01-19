import sqlite from "better-sqlite3";
import { describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { SQLiteConnection } from "../src/sqlite-connection";
import type { SQLiteDatabase } from "../src/sqlite-types";

describe("SQLiteConnection", () => {
  it("rewrites postgres-style params and absurd schema names", async () => {
    const db = new sqlite(":memory:") as SQLiteDatabase;
    const conn = new SQLiteConnection(db);

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

  it("throws when query is used for non-reader statements", async () => {
    const db = new sqlite(":memory:") as SQLiteDatabase;
    const conn = new SQLiteConnection(db);

    await expect(conn.query("CREATE TABLE t (id)")).rejects.toThrow(
      "only statements that return data"
    );
    db.close();
  });

  it("decodes JSON from typeless columns", async () => {
    const db = new sqlite(":memory:") as SQLiteDatabase;
    const conn = new SQLiteConnection(db);

    await conn.exec("CREATE TABLE t (payload)");
    await conn.exec("INSERT INTO t (payload) VALUES ($1)", ['{"a":1}']);

    const { rows } = await conn.query<{ payload: { a: number } }>(
      "SELECT payload FROM t"
    );

    expect(rows[0]?.payload).toEqual({ a: 1 });
    db.close();
  });

  it("decodes JSON from blob columns", async () => {
    const db = new sqlite(":memory:") as SQLiteDatabase;
    const conn = new SQLiteConnection(db);

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
    const db = new sqlite(":memory:") as SQLiteDatabase;
    const conn = new SQLiteConnection(db);
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

  it("allows custom value codec overrides", async () => {
    const db = new sqlite(":memory:") as SQLiteDatabase;
    const encodedValues: unknown[] = [];
    const conn = new SQLiteConnection(db, {
      valueCodec: {
        encodeParam: (value) => {
          encodedValues.push(value);
          return value;
        },
        decodeColumn: ({ value }) => {
          if (typeof value === "string") {
            return value.toUpperCase();
          }
          return value;
        },
      },
    });

    await conn.exec("CREATE TABLE t (name TEXT)");
    await conn.exec("INSERT INTO t (name) VALUES ($1)", ["alpha"]);

    const { rows } = await conn.query<{ name: string }>(
      "SELECT name FROM t"
    );

    expect(encodedValues).toEqual(["alpha"]);
    expect(rows[0]?.name).toBe("ALPHA");
    db.close();
  });

  it("retries when SQLite reports the database is busy", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "absurd-sqlite-busy-"));
    const dbPath = join(tempDir, "busy.db");
    const primary = new sqlite(dbPath) as SQLiteDatabase;
    (primary as any).pragma("busy_timeout = 1");
    const conn = new SQLiteConnection(primary);
    await conn.exec("CREATE TABLE t_busy (id INTEGER PRIMARY KEY, value TEXT)");

    const blocker = new sqlite(dbPath);
    blocker.pragma("busy_timeout = 1");
    blocker.exec("BEGIN EXCLUSIVE");

    let released = false;
    const releaseLock = () => {
      if (released) return;
      released = true;
      try {
        blocker.exec("COMMIT");
      } catch (err) {
        // Ignore if the transaction was already closed.
      }
      blocker.close();
    };
    const timer = setTimeout(releaseLock, 50);

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
      readonly: false,
      columns: vi.fn().mockReturnValue([]),
      all: vi.fn(),
      run: vi.fn(() => {
        attempts++;
        if (attempts === 1) {
          throw lockedError;
        }
        return 1;
      }),
    };

    const prepareSpy = vi.fn().mockReturnValue(statement as any);
    const db: SQLiteDatabase = {
      prepare: prepareSpy as any,
      close: vi.fn(),
      loadExtension: vi.fn(),
    };
    const conn = new SQLiteConnection(db);

    await expect(
      conn.exec("UPDATE locked_table SET value = $1 WHERE id = $2", [1, 1])
    ).resolves.toBeUndefined();
    expect(statement.run).toHaveBeenCalledTimes(2);
    expect(prepareSpy).toHaveBeenCalledTimes(1);
  });
});
