import type { Queryable } from "./absurd";
import type {
  SQLiteColumnDefinition,
  SQLiteDatabase,
  SQLiteStatement,
  SQLiteVerboseLog,
  SQLiteBindValue,
} from "./sqlite-types";

/**
 * Hooks for encoding parameters and decoding query results.
 * Useful when SQLite drivers expose different value representations.
 */
export interface SQLiteValueCodec {
  encodeParam?: (value: SQLiteBindValue) => SQLiteBindValue;
  decodeColumn?: (args: {
    value: unknown;
    columnName: string;
    columnType: string | null;
    verbose?: SQLiteVerboseLog;
  }) => unknown;
  decodeRow?: (args: {
    row: Record<string, unknown>;
    columns: SQLiteColumnDefinition[];
    decodeColumn: NonNullable<SQLiteValueCodec["decodeColumn"]>;
    verbose?: SQLiteVerboseLog;
  }) => Record<string, unknown>;
}

/**
 * Configuration options for SQLiteConnection.
 */
export interface SQLiteConnectionOptions {
  valueCodec?: SQLiteValueCodec;
  verbose?: SQLiteVerboseLog;
}

/**
 * SQLite adapter that rewrites Absurd's SQL to SQLite syntax and handles retries.
 */
export class SQLiteConnection implements Queryable {
  private readonly db: SQLiteDatabase;
  private readonly maxRetries = 5;
  private readonly baseRetryDelayMs = 50;
  private readonly codec: Required<Pick<SQLiteValueCodec, "encodeParam" | "decodeColumn">> &
    Pick<SQLiteValueCodec, "decodeRow">;
  private readonly verbose?: SQLiteVerboseLog;

  constructor(db: SQLiteDatabase, options: SQLiteConnectionOptions = {}) {
    this.db = db;
    this.codec = {
      encodeParam: options.valueCodec?.encodeParam ?? encodeColumnValue,
      decodeColumn: options.valueCodec?.decodeColumn ?? decodeColumnValue,
      decodeRow: options.valueCodec?.decodeRow,
    };
    this.verbose = options.verbose;
  }

  async query<R extends object = Record<string, any>>(
    sql: string,
    params?: unknown[] | Record<string, unknown>
  ): Promise<{ rows: R[] }> {
    const sqliteQuery = rewritePostgresQuery(sql);
    const sqliteParams = rewritePostgresParams(params, this.codec.encodeParam);

    const statement = this.db.prepare(sqliteQuery);
    if (!statement.readonly) {
      // this indicates `return_data` is false
      // https://github.com/WiseLibs/better-sqlite3/blob/6209be238d6a1b181f516e4e636986604b0f62e1/src/objects/statement.cpp#L134C83-L134C95
      throw new Error("The query() method is only statements that return data");
    }

    const rowsDecoded = await this.runWithRetry(() => {
      const rows = statement.all(sqliteParams);
      return rows.map((row) =>
        decodeRowValues(statement, row, this.codec, this.verbose)
      );
    });

    return { rows: rowsDecoded };
  }

  async exec(
    sql: string,
    params?: unknown[] | Record<string, unknown>
  ): Promise<void> {
    const sqliteQuery = rewritePostgresQuery(sql);
    const sqliteParams = rewritePostgresParams(params, this.codec.encodeParam);

    const statement = this.db.prepare(sqliteQuery);
    await this.runWithRetry(() => statement.run(sqliteParams));
  }

  close(): void {
    this.db.close();
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

const namedParamPrefix = "p";

function rewritePostgresQuery(text: string): string {
  return text
    .replace(/\$(\d+)/g, `:${namedParamPrefix}$1`)
    .replace(/absurd\.(\w+)/g, "absurd_$1");
}

function rewritePostgresParams(
  params: unknown[] | Record<string, unknown> | undefined,
  encodeParam: (value: SQLiteBindValue) => SQLiteBindValue
): Record<string, SQLiteBindValue> {
  if (!params) {
    return {};
  }

  const rewrittenParams: Record<string, SQLiteBindValue> = {};
  if (Array.isArray(params)) {
    params.forEach((value, index) => {
      const paramKey = `${namedParamPrefix}${index + 1}`;
      const encodedParamValue = encodeParam(value as SQLiteBindValue);

      rewrittenParams[paramKey] = encodedParamValue;
    });
    return rewrittenParams;
  }

  for (const [key, value] of Object.entries(params)) {
    rewrittenParams[key] = encodeParam(value as SQLiteBindValue);
  }
  return rewrittenParams;
}

function decodeRowValues<U extends object, R extends object = any>(
  statement: SQLiteStatement,
  row: U,
  codec: Required<Pick<SQLiteValueCodec, "decodeColumn">> &
    Pick<SQLiteValueCodec, "decodeRow">,
  verbose?: SQLiteVerboseLog
): R {
  const columns = statement.columns();
  const rowRecord = row as Record<string, unknown>;

  if (codec.decodeRow) {
    return codec.decodeRow({
      row: rowRecord,
      columns,
      decodeColumn: codec.decodeColumn,
      verbose,
    }) as R;
  }

  const decodedRow: any = {};
  for (const column of columns) {
    const columnName = column.name;
    const columnType = column.type;
    const rawValue = rowRecord[columnName];
    const decodedValue = codec.decodeColumn({
      value: rawValue,
      columnName,
      columnType,
      verbose,
    });
    decodedRow[columnName] = decodedValue;
  }

  return decodedRow as R;
}

function decodeColumnValue<V = any>(args: {
  value: unknown | V;
  columnName: string;
  columnType: string | null;
  verbose?: SQLiteVerboseLog;
}): V | null {
  const { value, columnName, columnType, verbose } = args;
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
