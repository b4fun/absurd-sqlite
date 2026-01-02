import assert from "node:assert/strict";
import { Buffer } from "node:buffer";

import { Database, DenoSqliteDatabase } from "../mod.ts";
import { SqliteConnection } from "../sqlite.ts";

Deno.test("sqlite: rewrites postgres-style params and absurd schema names", async () => {
  const db = new DenoSqliteDatabase(new Database(":memory:"));
  const conn = new SqliteConnection(db);

  await conn.exec("CREATE TABLE absurd_tasks (id, name)");
  await conn.exec("INSERT INTO absurd.tasks (id, name) VALUES ($1, $2)", [
    1,
    "alpha",
  ]);

  const { rows } = await conn.query<{ id: number; name: string }>(
    "SELECT id, name FROM absurd.tasks WHERE id = $1",
    [1],
  );

  assert.deepEqual(rows, [{ id: 1, name: "alpha" }]);
  db.close();
});

Deno.test("sqlite: throws when query is used for non-reader statements", async () => {
  const db = new DenoSqliteDatabase(new Database(":memory:"));
  const conn = new SqliteConnection(db);

  await assert.rejects(
    () => conn.query("CREATE TABLE t (id)"),
    (err: unknown) =>
      String((err as { message?: string })?.message ?? "").includes(
        "only statements that return data",
      ),
  );
  db.close();
});

Deno.test("sqlite: decodes JSON from typeless columns", async () => {
  const db = new DenoSqliteDatabase(new Database(":memory:"));
  const conn = new SqliteConnection(db);

  await conn.exec("CREATE TABLE t (payload)");
  await conn.exec("INSERT INTO t (payload) VALUES ($1)", ['{"a":1}']);

  const { rows } = await conn.query<{ payload: { a: number } }>(
    "SELECT payload FROM t",
  );

  assert.deepEqual(rows[0]?.payload, { a: 1 });
  db.close();
});

Deno.test("sqlite: decodes JSON from blob columns", async () => {
  const db = new DenoSqliteDatabase(new Database(":memory:"));
  const conn = new SqliteConnection(db);

  await conn.exec("CREATE TABLE t_blob (payload BLOB)");
  await conn.exec("INSERT INTO t_blob (payload) VALUES ($1)", [
    Buffer.from(JSON.stringify({ b: 2 })),
  ]);

  const { rows } = await conn.query<{ payload: { b: number } }>(
    "SELECT payload FROM t_blob",
  );

  assert.deepEqual(rows[0]?.payload, { b: 2 });
  db.close();
});

Deno.test("sqlite: decodes datetime columns into Date objects", async () => {
  const db = new DenoSqliteDatabase(new Database(":memory:"));
  const conn = new SqliteConnection(db);
  const now = Date.now();

  await conn.exec("CREATE TABLE t_date (created_at DATETIME)");
  await conn.exec("INSERT INTO t_date (created_at) VALUES ($1)", [now]);

  const { rows } = await conn.query<{ created_at: Date }>(
    "SELECT created_at FROM t_date",
  );

  assert(rows[0]?.created_at instanceof Date);
  assert.equal(rows[0]?.created_at.getTime(), now);
  db.close();
});
