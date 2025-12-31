import assert from "node:assert/strict";

export function assertMatchObject(
  actual: object | null | undefined,
  expected: Record<string, unknown>,
): void {
  assert(actual, "Expected object to be defined");
  const actualRecord = actual as Record<string, unknown>;
  matchObject(actualRecord, expected);
}

export function assertArrayContains<T>(
  actual: T[],
  expected: T[],
): void {
  for (const value of expected) {
    assert(
      actual.includes(value),
      `Expected array to contain ${String(value)}`,
    );
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(
    value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      !(value instanceof Date),
  );
}

function matchObject(
  actual: Record<string, unknown>,
  expected: Record<string, unknown>,
): void {
  for (const [key, value] of Object.entries(expected)) {
    const actualValue = actual[key];
    if (isPlainObject(value) && isPlainObject(actualValue)) {
      matchObject(actualValue, value);
      continue;
    }
    assert.deepEqual(actualValue, value, `Expected ${key} to match`);
  }
}

export async function assertRejectsMessage(
  promise: Promise<unknown>,
  message: string,
): Promise<void> {
  await assert.rejects(promise, (err: unknown) => {
    const text = String((err as { message?: string })?.message ?? err);
    return text.includes(message);
  });
}

export async function waitFor(
  fn: () => void | boolean | Promise<void | boolean>,
  options?: { timeoutMs?: number; intervalMs?: number },
): Promise<void> {
  const timeoutMs = options?.timeoutMs ?? 200;
  const intervalMs = options?.intervalMs ?? 5;
  const start = Date.now();

  while (true) {
    try {
      const result = await fn();
      if (result === false) {
        throw new Error("Condition not met");
      }
      return;
    } catch (err) {
      if (Date.now() - start >= timeoutMs) {
        throw err;
      }
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }
}

type AnyFunction = (...args: unknown[]) => unknown;

export interface Spy {
  calls: unknown[][];
  restore: () => void;
  mockRejectedValueOnce: (error: unknown) => void;
  mockResolvedValueOnce: (value: unknown) => void;
  mockImplementationOnce: (impl: AnyFunction) => void;
}

const activeSpies: Array<{ restore: () => void }> = [];

export function restoreAllSpies(): void {
  for (const spy of activeSpies.splice(0)) {
    spy.restore();
  }
}

export function createSpy<T extends object, K extends keyof T>(
  target: T,
  key: K,
): Spy {
  const original = target[key] as unknown as AnyFunction;
  const calls: unknown[][] = [];
  const overrides: Array<AnyFunction> = [];

  const wrapper = function (this: T, ...args: unknown[]) {
    calls.push(args);
    if (overrides.length > 0) {
      const next = overrides.shift();
      return next?.apply(this, args);
    }
    return original.apply(this, args);
  };

  (target as Record<string, unknown>)[key as string] = wrapper as T[K];

  const restore = () => {
    (target as Record<string, unknown>)[key as string] = original as T[K];
  };

  const spy: Spy = {
    calls,
    restore,
    mockRejectedValueOnce: (error: unknown) => {
      overrides.push(() => Promise.reject(error));
    },
    mockResolvedValueOnce: (value: unknown) => {
      overrides.push(() => Promise.resolve(value));
    },
    mockImplementationOnce: (impl: AnyFunction) => {
      overrides.push(impl);
    },
  };

  activeSpies.push(spy);
  return spy;
}

export function useFakeTime(start: Date): {
  set: (next: Date) => void;
  restore: () => void;
} {
  const realNow = Date.now;
  let current = start.getTime();
  Date.now = () => current;
  return {
    set: (next: Date) => {
      current = next.getTime();
    },
    restore: () => {
      Date.now = realNow;
    },
  };
}

export function resolved<T>(value: T): Promise<T> {
  return Promise.resolve(value);
}

export function rejected<T = never>(error: unknown): Promise<T> {
  return Promise.reject(error);
}
