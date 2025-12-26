import sqlite from "better-sqlite3";

import { Queryable } from "./absurd";

type EnforceIdenticalKeys<T, U> = keyof T extends keyof U
  ? keyof U extends keyof T
    ? T
    : never
  : never;

export class SqliteConnection implements Queryable {
  private readonly db: sqlite.Database;

  constructor(db: sqlite.Database) {
    this.db = db;
  }

  async query<R extends object, I = any>(
    sql: string,
    params?: I[]
  ): Promise<{ rows: R[] }> {
    const sqliteQuery = rewritePostgresQuery(sql);
    const sqliteParams = rewritePostgresParams(params);

    const statement = this.db.prepare(sqliteQuery);
    if (!statement.reader) {
      // this indicates `return_data` is false
      // https://github.com/WiseLibs/better-sqlite3/blob/6209be238d6a1b181f516e4e636986604b0f62e1/src/objects/statement.cpp#L134C83-L134C95
      throw new Error("The query() method is only statements that return data");
    }

    const rows = statement.all(sqliteParams) as EnforceIdenticalKeys<
      sqlite.RunResult,
      R
    >[];
    const rowsDecoded = rows.map((row) => decodeRowValues(statement, row));

    return { rows: rowsDecoded };
  }

  async exec<I = any>(sql: string, params?: I[]): Promise<void> {
    const sqliteQuery = rewritePostgresQuery(sql);
    const sqliteParams = rewritePostgresParams(params);

    this.db.prepare(sqliteQuery).run(sqliteParams);
  }
}

const namedParamPrefix = "p";

function rewritePostgresQuery(text: string): string {
  return text
    .replace(/\$(\d+)/g, `:${namedParamPrefix}$1`)
    .replace(/absurd\.(\w+)/g, "absurd_$1");
}

function rewritePostgresParams<I = any>(params?: I[]): Record<string, I> {
  if (!params) {
    return {};
  }

  const rewrittenParams: Record<string, I> = {};
  params.forEach((value, index) => {
    const paramKey = `${namedParamPrefix}${index + 1}`;
    const encodedParamValue = encodeColumnValue(value);

    rewrittenParams[paramKey] = encodedParamValue;
  });
  return rewrittenParams;
}

function decodeRowValues<U extends object, R extends object = any>(
  statement: sqlite.Statement,
  row: EnforceIdenticalKeys<U, R>,
  verbose?: sqlite.Options["verbose"]
): R {
  const columns = statement.columns();

  const decodedRow: any = {};
  for (const column of columns) {
    const columnName = column.name;
    const columnType = column.type;
    const rawValue = (row as Record<string, unknown>)[columnName];
    const decodedValue = decodeColumnValue(
      rawValue,
      columnName,
      columnType,
      verbose
    );
    decodedRow[columnName] = decodedValue;
  }

  return decodedRow as R;
}

function decodeColumnValue<V = any>(
  value: unknown | V,
  columnName: string,
  columnType: string | null,
  verbose?: sqlite.Options["verbose"]
): V | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (columnType === null) {
    if (typeof value === "string") {
      // When column type is not known but the value is string
      // try parse it as JSON -- for cases where the column is computed
      // e.g. `SELECT json(x) as y from ....`
      // FIXME: better type detection
      let rv: V;
      let isValidJSON = false;
      try {
        rv = JSON.parse(value) as V;
        isValidJSON = true;
      } catch (e) {
        verbose?.(`Failed to decode string column ${columnName} as JSON`, e);
        rv = value as V;
      }
      if (isValidJSON) {
        verbose?.(`Decoded column ${columnName} with null as JSON`);
      }
      return rv;
    }

    verbose?.(`Column ${columnName} has null type, returning raw value`);
    return value as V;
  }

  const columnTypeName = columnType.toLowerCase();
  if (columnTypeName === "blob") {
    // BLOB values are JSON string decoded from JSONB
    try {
      return JSON.parse(value.toString()) as V;
    } catch (e) {
      verbose?.(`Failed to decode BLOB column ${columnName} as JSON`, e);
      throw e;
    }
  }

  if (columnTypeName === "datetime") {
    if (typeof value !== "number") {
      throw new Error(
        `Expected datetime column ${columnName} to be a number, got ${typeof value}`
      );
    }
    return new Date(value) as V;
  }

  // For other types, return as is
  return value as V;
}

function encodeColumnValue(value: any): any {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "number" && Number.isInteger(value)) {
    return value.toString();
  }
  return value;
}
