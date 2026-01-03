import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";

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
});
