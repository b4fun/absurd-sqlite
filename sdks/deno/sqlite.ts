import type { Queryable } from "./absurd-types.ts";
import type {
  SQLiteBindParams,
  SQLiteBindValue,
  SQLiteDatabase,
  SQLiteRestBindParams,
  SQLiteStatement,
  SQLiteVerboseLog,
} from "./sqlite-types.ts";

export class SqliteConnection implements Queryable {
  private readonly db: SQLiteDatabase;

  constructor(db: SQLiteDatabase) {
    this.db = db;
    // TODO: verbose logging
  }

  query<R extends object = Record<string, unknown>>(
    sql: string,
    params?: SQLiteRestBindParams,
  ): Promise<{ rows: R[] }> {
    const sqliteQuery = rewritePostgresQuery(sql);
    const sqliteParams = rewritePostgresParams(params);

    const statement = this.db.prepare(sqliteQuery);
    if (!statement.readonly) {
      // This indicates `return_data` is false
      // https://github.com/WiseLibs/better-sqlite3/blob/6209be238d6a1b181f516e4e636986604b0f62e1/src/objects/statement.cpp#L134C83-L134C95
      return Promise.reject(
        new Error("The query() method is only statements that return data"),
      );
    }

    const rowsDecoded = statement
      .all(sqliteParams)
      .map((row) => decodeRowValues(statement, row)) as R[];

    return Promise.resolve({ rows: rowsDecoded });
  }

  exec(sql: string, params?: SQLiteRestBindParams): Promise<void> {
    const sqliteQuery = rewritePostgresQuery(sql);
    const sqliteParams = rewritePostgresParams(params);

    this.db.prepare(sqliteQuery).run(sqliteParams);
    return Promise.resolve();
  }
}

const namedParamPrefix = "p";

function rewritePostgresQuery(text: string): string {
  return text
    .replace(/\$(\d+)/g, `:${namedParamPrefix}$1`)
    .replace(/absurd\.(\w+)/g, "absurd_$1");
}

function rewritePostgresParams(
  params?: SQLiteRestBindParams,
): SQLiteBindParams {
  if (!params) {
    return {};
  }

  if (!Array.isArray(params)) {
    return params as SQLiteBindParams;
  }

  const rewrittenParams: Record<string, SQLiteBindValue> = {};
  params.forEach((value, index) => {
    const paramKey = `${namedParamPrefix}${index + 1}`;
    const encodedParamValue = encodeColumnValue(value);

    rewrittenParams[paramKey] = encodedParamValue as SQLiteBindValue;
  });
  return rewrittenParams;
}

function decodeRowValues<
  U extends object,
  R extends object = Record<string, unknown>,
>(
  statement: SQLiteStatement,
  row: U,
  verbose?: SQLiteVerboseLog,
): R {
  const columns = statement.columns();

  const decodedRow: Record<string, unknown> = {};
  for (const column of columns) {
    const columnName = column.name;
    const columnType = column.type;
    const rawValue = (row as Record<string, unknown>)[columnName];
    const decodedValue = decodeColumnValue(
      rawValue,
      columnName,
      columnType,
      verbose,
    );
    decodedRow[columnName] = decodedValue;
  }

  return decodedRow as R;
}

function decodeColumnValue<V = unknown>(
  value: unknown | V,
  columnName: string,
  columnType: string | null,
  verbose?: SQLiteVerboseLog,
): V | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (columnType === null) {
    if (value instanceof Uint8Array) {
      const raw = new TextDecoder().decode(value);
      try {
        return JSON.parse(raw) as V;
      } catch (e) {
        verbose?.(`Failed to decode bytes column ${columnName} as JSON`, e);
        return raw as V;
      }
    }

    if (isDateLikeColumn(columnName)) {
      const parsed = parseDateValue(value, columnName, verbose);
      if (parsed) {
        return parsed as V;
      }
    }

    if (typeof value === "bigint") {
      return normalizeBigInt(value) as V;
    }

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
      const raw = value instanceof Uint8Array
        ? new TextDecoder().decode(value)
        : value.toString();
      return JSON.parse(raw) as V;
    } catch (e) {
      verbose?.(`Failed to decode BLOB column ${columnName} as JSON`, e);
      throw e;
    }
  }

  if (columnTypeName === "datetime") {
    const parsed = parseDateValue(value, columnName, verbose);
    if (parsed) {
      return parsed as V;
    }
    throw new Error(
      `Expected datetime column ${columnName} to be a number or string, got ${typeof value}`,
    );
  }

  // For other types, return as is
  if (typeof value === "bigint") {
    return normalizeBigInt(value) as V;
  }
  return value as V;
}

const dateColumnSuffixes = ["_at", "_time"];

function isDateLikeColumn(name: string): boolean {
  return dateColumnSuffixes.some((suffix) => name.endsWith(suffix));
}

function parseDateValue(
  value: unknown,
  columnName: string,
  verbose?: SQLiteVerboseLog,
): Date | null {
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === "number") {
    return new Date(value);
  }
  if (typeof value === "bigint") {
    return new Date(Number(value));
  }
  if (typeof value === "string") {
    const numeric = Number(value);
    if (!Number.isNaN(numeric)) {
      return new Date(numeric);
    }
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
    verbose?.(`Failed to decode datetime column ${columnName} from string`);
  }
  return null;
}

function normalizeBigInt(value: bigint): number | bigint {
  const asNumber = Number(value);
  if (Number.isSafeInteger(asNumber)) {
    return asNumber;
  }
  return value;
}

function encodeColumnValue(value: unknown): SQLiteBindValue {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "number" && Number.isInteger(value)) {
    return value.toString();
  }
  return value as SQLiteBindValue;
}
