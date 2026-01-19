import { Database } from "bun:sqlite";

import type {
  SQLiteBindValue,
  SQLiteColumnDefinition,
  SQLiteConnectionOptions,
  SQLiteDatabase,
  SQLiteStatement,
  SQLiteValueCodec,
} from "@absurd-sqlite/sdk";
import { SQLiteConnection } from "@absurd-sqlite/sdk";

export class BunSqliteConnection extends SQLiteConnection {
  constructor(db: Database, options: SQLiteConnectionOptions = {}) {
    const valueCodec = buildValueCodec(options.valueCodec);
    super(new BunSqliteDatabase(db), { ...options, valueCodec });
  }
}

class BunSqliteDatabase implements SQLiteDatabase {
  constructor(private readonly db: Database) {}

  prepare<Result extends object = Record<string, any>>(
    sql: string
  ): SQLiteStatement<Result> {
    const statement = this.db.prepare(sql);
    return new BunSqliteStatement(statement, isReadonlyQuery(sql));
  }

  close(): void {
    this.db.close();
  }

  loadExtension(path: string): void {
    (this.db as unknown as { loadExtension(path: string): void }).loadExtension(
      path
    );
  }
}

class BunSqliteStatement<Result extends object = Record<string, any>>
  implements SQLiteStatement<Result>
{
  readonly readonly: boolean;

  constructor(
    private readonly stmt: ReturnType<Database["prepare"]>,
    readonlyFlag: boolean
  ) {
    this.readonly = readonlyFlag;
  }

  columns(): SQLiteColumnDefinition[] {
    const columnNames = this.stmt.columnNames ?? [];
    const declaredTypes = this.stmt.declaredTypes ?? [];
    return columnNames.map((name, index) => ({
      name,
      column: null,
      table: null,
      database: null,
      type: normalizeColumnType(declaredTypes[index] ?? null),
    }));
  }

  all(...args: any[]): Result[] {
    const normalizedArgs = normalizeStatementArgs(args);
    return this.stmt.all(...normalizedArgs) as Result[];
  }

  run(...args: any[]): unknown {
    const normalizedArgs = normalizeStatementArgs(args);
    return this.stmt.run(...normalizedArgs);
  }
}

function buildValueCodec(
  overrides?: SQLiteValueCodec
): SQLiteValueCodec {
  return {
    encodeParam: overrides?.encodeParam ?? encodeColumnValue,
    decodeColumn: overrides?.decodeColumn ?? decodeColumnValue,
    decodeRow: overrides?.decodeRow ?? decodeRowValues,
  };
}

function normalizeStatementArgs(args: any[]): any[] {
  if (args.length !== 1) {
    return args;
  }
  const params = args[0];
  if (!params || typeof params !== "object" || Array.isArray(params)) {
    return args;
  }
  const normalized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(params)) {
    normalized[normalizeParamKey(key)] = value;
  }
  return [normalized];
}

function normalizeParamKey(key: string): string {
  if (key.startsWith("$") || key.startsWith(":") || key.startsWith("@")) {
    return key;
  }
  return `:${key}`;
}

function decodeRowValues<R extends object = any>(args: {
  row: Record<string, unknown>;
  columns?: SQLiteColumnDefinition[];
  decodeColumn?: (args: {
    value: unknown;
    columnName: string;
    columnType: string | null;
  }) => unknown;
}): R {
  const decodedRow: any = {};
  for (const [columnName, rawValue] of Object.entries(args.row)) {
    decodedRow[columnName] = decodeColumnValue({
      value: rawValue,
      columnName,
      columnType: null,
    });
  }

  return decodedRow as R;
}

function decodeColumnValue<V = any>(args: {
  value: unknown | V;
  columnName: string;
  columnType: string | null;
  verbose?: (...args: any[]) => void;
}): V | null {
  const { value, columnName } = args;
  if (value === null || value === undefined) {
    return null;
  }

  if (isTimestampColumn(columnName)) {
    if (typeof value === "number") {
      return new Date(value) as V;
    }
    if (typeof value === "string") {
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
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
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

function encodeColumnValue(value: SQLiteBindValue): SQLiteBindValue {
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

function isReadonlyQuery(sql: string): boolean {
  const trimmed = sql.trim().toLowerCase();
  return (
    trimmed.startsWith("select") ||
    trimmed.startsWith("with") ||
    trimmed.startsWith("pragma") ||
    trimmed.startsWith("explain")
  );
}

function normalizeColumnType(value: string | null): string | null {
  if (!value) {
    return null;
  }
  const lowered = value.toLowerCase();
  if (lowered === "null") {
    return null;
  }
  return lowered;
}
