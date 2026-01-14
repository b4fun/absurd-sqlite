import { Database } from "bun:sqlite";

import type { Queryable } from "@absurd-sqlite/sdk";
import type {
  SQLiteBindParams,
  SQLiteBindValue,
  SQLiteRestBindParams,
} from "@absurd-sqlite/sdk";

export class BunSqliteConnection implements Queryable {
  private readonly db: Database;
  private readonly maxRetries = 5;
  private readonly baseRetryDelayMs = 50;

  constructor(db: Database) {
    this.db = db;
  }

  async query<R extends object = Record<string, any>>(
    sql: string,
    params?: SQLiteRestBindParams
  ): Promise<{ rows: R[] }> {
    const { sql: sqliteQuery, paramOrder } = rewritePostgresQuery(sql);
    const sqliteParams = rewritePostgresParams(
      normalizeParams(params),
      paramOrder
    );

    const statement = this.db.query(sqliteQuery);
    const rows = await this.runWithRetry(() =>
      statement.all(...sqliteParams).map((row) =>
        decodeRowValues(row as Record<string, unknown>)
      )
    );

    return { rows: rows as R[] };
  }

  async exec(sql: string, params?: SQLiteRestBindParams): Promise<void> {
    const { sql: sqliteQuery, paramOrder } = rewritePostgresQuery(sql);
    const sqliteParams = rewritePostgresParams(
      normalizeParams(params),
      paramOrder
    );

    const statement = this.db.query(sqliteQuery);
    await this.runWithRetry(() => statement.run(...sqliteParams));
  }

  private async runWithRetry<T>(operation: () => T): Promise<T> {
    let attempt = 0;
    while (true) {
      try {
        return operation();
      } catch (err) {
        if (!isRetryableSQLiteError(err) || attempt >= this.maxRetries) {
          throw err;
        }
        attempt++;
        await delay(this.baseRetryDelayMs * attempt);
      }
    }
  }
}

function rewritePostgresQuery(text: string): {
  sql: string;
  paramOrder: number[];
} {
  const paramOrder: number[] = [];
  const sql = text
    .replace(/\$(\d+)/g, (_, index) => {
      paramOrder.push(Number(index));
      return "?";
    })
    .replace(/absurd\.(\w+)/g, "absurd_$1");

  return { sql, paramOrder };
}

function rewritePostgresParams<I = any>(
  params: SQLiteBindValue[],
  paramOrder: number[]
): I[] {
  if (paramOrder.length === 0) {
    return params.map((value) => encodeColumnValue(value)) as I[];
  }

  return paramOrder.map((index) => {
    const value = params[index - 1];
    return encodeColumnValue(value) as I;
  });
}

function decodeRowValues<R extends object = any>(
  row: Record<string, unknown>
): R {
  const decodedRow: any = {};
  for (const [columnName, rawValue] of Object.entries(row)) {
    decodedRow[columnName] = decodeColumnValue(rawValue, columnName);
  }

  return decodedRow as R;
}

function decodeColumnValue<V = any>(
  value: unknown | V,
  columnName: string
): V | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (isTimestampColumn(columnName)) {
    if (typeof value === "number") {
      return new Date(value) as V;
    }
    if (typeof value === "string") {
      // Try parsing as Unix timestamp in milliseconds (stored as string)
      const numValue = parseInt(value, 10);
      if (!Number.isNaN(numValue)) {
        return new Date(numValue) as V;
      }
      // Fallback to Date.parse for ISO strings or other valid date formats
      const parsed = Date.parse(value);
      if (!Number.isNaN(parsed)) {
        return new Date(parsed) as V;
      }
    }
  }

  if (typeof value === "string") {
    return tryDecodeJson(value) ?? (value as V);
  }

  if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
    const bytes =
      value instanceof Uint8Array ? value : new Uint8Array(value);
    const decoded = new TextDecoder().decode(bytes);
    return tryDecodeJson(decoded) ?? (value as V);
  }

  return value as V;
}

function tryDecodeJson<V = any>(value: string): V | null {
  try {
    return JSON.parse(value) as V;
  } catch {
    return null;
  }
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

function isTimestampColumn(columnName: string): boolean {
  return columnName.endsWith("_at");
}

function normalizeParams(
  params?: SQLiteRestBindParams
): SQLiteBindValue[] {
  if (!params) {
    return [];
  }

  if (params.length === 1 && isBindParams(params[0])) {
    const inner = params[0];
    if (Array.isArray(inner)) {
      return inner;
    }
    return Object.values(inner);
  }

  return params as SQLiteBindValue[];
}

function isBindParams(value: unknown): value is SQLiteBindParams {
  if (Array.isArray(value)) {
    return true;
  }
  if (!value || typeof value !== "object") {
    return false;
  }
  const tag = Object.prototype.toString.call(value);
  return tag === "[object Object]";
}

const sqliteRetryableErrorCodes = new Set(["SQLITE_BUSY", "SQLITE_LOCKED"]);
const sqliteRetryableErrnos = new Set([5, 6]);

function isRetryableSQLiteError(err: unknown): boolean {
  if (!err || typeof err !== "object") {
    return false;
  }

  const code = (err as any).code;
  if (typeof code === "string") {
    for (const retryableCode of sqliteRetryableErrorCodes) {
      if (code.startsWith(retryableCode)) {
        return true;
      }
    }
  }

  const errno = (err as any).errno;
  if (typeof errno === "number" && sqliteRetryableErrnos.has(errno)) {
    return true;
  }

  return false;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
