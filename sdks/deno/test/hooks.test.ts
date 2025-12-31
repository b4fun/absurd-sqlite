import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";

import { withTestContext } from "./setup.ts";
import { resolved } from "./utils.ts";

Deno.test("hooks: beforeSpawn injects headers", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    const absurd = ctx.createClient({
      queueName: ctx.queueName,
      hooks: {
        beforeSpawn: (_taskName, _params, options) => {
          return {
            ...options,
            headers: {
              ...options.headers,
              traceId: "trace-123",
              correlationId: "corr-456",
            },
          };
        },
      },
    });

    let capturedHeaders: Record<string, unknown> | null = null;
    absurd.registerTask(
      { name: "capture-headers" },
      (_params, taskCtx) => {
        capturedHeaders = taskCtx.headers as Record<string, unknown>;
        return resolved("done");
      },
    );

    await absurd.spawn("capture-headers", { test: true });
    await absurd.workBatch("worker1", 60, 1);

    assert.deepEqual(capturedHeaders, {
      traceId: "trace-123",
      correlationId: "corr-456",
    });
  });
});

Deno.test("hooks: async beforeSpawn hook", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    const absurd = ctx.createClient({
      queueName: ctx.queueName,
      hooks: {
        beforeSpawn: async (_taskName, _params, options) => {
          await new Promise((resolve) => setTimeout(resolve, 10));
          return {
            ...options,
            headers: {
              ...options.headers,
              asyncHeader: "fetched-value",
            },
          };
        },
      },
    });

    let capturedHeader: unknown = null;
    absurd.registerTask({ name: "async-header" }, (_params, taskCtx) => {
      capturedHeader = taskCtx.headers["asyncHeader"];
      return resolved("done");
    });

    await absurd.spawn("async-header", {});
    await absurd.workBatch("worker1", 60, 1);

    assert.equal(capturedHeader, "fetched-value");
  });
});

Deno.test("hooks: preserves existing headers when adding new ones", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    const absurd = ctx.createClient({
      queueName: ctx.queueName,
      hooks: {
        beforeSpawn: (_taskName, _params, options) => {
          return {
            ...options,
            headers: {
              ...options.headers,
              injected: "by-hook",
            },
          };
        },
      },
    });

    let capturedHeaders: Record<string, unknown> | null = null;
    absurd.registerTask({ name: "merge-headers" }, (_params, taskCtx) => {
      capturedHeaders = taskCtx.headers as Record<string, unknown>;
      return resolved("done");
    });

    await absurd.spawn(
      "merge-headers",
      {},
      {
        headers: { existing: "user-provided" },
      },
    );
    await absurd.workBatch("worker1", 60, 1);

    assert.deepEqual(capturedHeaders, {
      existing: "user-provided",
      injected: "by-hook",
    });
  });
});

Deno.test("hooks: wrapTaskExecution wraps task execution", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    const executionOrder: string[] = [];

    const absurd = ctx.createClient({
      queueName: ctx.queueName,
      hooks: {
        wrapTaskExecution: async (_taskCtx, execute) => {
          executionOrder.push("before");
          const result = await execute();
          executionOrder.push("after");
          return result;
        },
      },
    });

    absurd.registerTask({ name: "wrapped-task" }, () => {
      executionOrder.push("handler");
      return resolved("done");
    });

    await absurd.spawn("wrapped-task", {});
    await absurd.workBatch("worker1", 60, 1);

    assert.deepEqual(executionOrder, ["before", "handler", "after"]);
  });
});

Deno.test("hooks: provides TaskContext to wrapper", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    let capturedTaskId: string | null = null;
    let capturedHeaders: Record<string, unknown> | null = null;

    const absurd = ctx.createClient({
      queueName: ctx.queueName,
      hooks: {
        beforeSpawn: (_taskName, _params, options) => ({
          ...options,
          headers: { traceId: "from-spawn" },
        }),
        wrapTaskExecution: (taskCtx, execute) => {
          capturedTaskId = taskCtx.taskID;
          capturedHeaders = taskCtx.headers as Record<string, unknown>;
          return execute();
        },
      },
    });

    absurd.registerTask(
      { name: "ctx-in-wrapper" },
      () => resolved("done"),
    );

    const { taskID } = await absurd.spawn("ctx-in-wrapper", {});
    await absurd.workBatch("worker1", 60, 1);

    assert.equal(capturedTaskId, taskID);
    assert.deepEqual(capturedHeaders, { traceId: "from-spawn" });
  });
});

Deno.test("hooks: ALS round-trip inject and restore", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    interface TraceContext {
      traceId: string;
      spanId: string;
    }
    const als = new AsyncLocalStorage<TraceContext>();

    const absurd = ctx.createClient({
      queueName: ctx.queueName,
      hooks: {
        beforeSpawn: (_taskName, _params, options) => {
          const store = als.getStore();
          if (store) {
            return {
              ...options,
              headers: {
                ...options.headers,
                traceId: store.traceId,
                spanId: store.spanId,
              },
            };
          }
          return options;
        },
        wrapTaskExecution: (taskCtx, execute) => {
          const traceId = taskCtx.headers["traceId"] as string | undefined;
          const spanId = taskCtx.headers["spanId"] as string | undefined;
          if (traceId && spanId) {
            return als.run({ traceId, spanId }, execute);
          }
          return execute();
        },
      },
    });

    let capturedInHandler: TraceContext | undefined;
    absurd.registerTask({ name: "als-test" }, () => {
      capturedInHandler = als.getStore();
      return resolved("done");
    });

    await als.run({ traceId: "trace-abc", spanId: "span-xyz" }, async () => {
      await absurd.spawn("als-test", {});
    });

    await absurd.workBatch("worker1", 60, 1);

    assert.deepEqual(capturedInHandler, {
      traceId: "trace-abc",
      spanId: "span-xyz",
    });
  });
});

Deno.test("hooks: child spawns inherit context from parent task", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    interface TraceContext {
      traceId: string;
    }
    const als = new AsyncLocalStorage<TraceContext>();

    const absurd = ctx.createClient({
      queueName: ctx.queueName,
      hooks: {
        beforeSpawn: (_taskName, _params, options) => {
          const store = als.getStore();
          if (store) {
            return {
              ...options,
              headers: {
                ...options.headers,
                traceId: store.traceId,
              },
            };
          }
          return options;
        },
        wrapTaskExecution: (taskCtx, execute) => {
          const traceId = taskCtx.headers["traceId"] as string | undefined;
          if (traceId) {
            return als.run({ traceId }, execute);
          }
          return execute();
        },
      },
    });

    let childTraceId: string | undefined;

    absurd.registerTask({ name: "parent-task" }, async () => {
      await absurd.spawn("child-task", {});
      return "parent-done";
    });

    absurd.registerTask({ name: "child-task" }, (_params, taskCtx) => {
      childTraceId = taskCtx.headers["traceId"] as string | undefined;
      return resolved("child-done");
    });

    await als.run({ traceId: "parent-trace" }, async () => {
      await absurd.spawn("parent-task", {});
    });

    await absurd.workBatch("worker1", 60, 1);
    await absurd.workBatch("worker1", 60, 1);

    assert.equal(childTraceId, "parent-trace");
  });
});

Deno.test("hooks: headers returns undefined for missing key", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    const absurd = ctx.createClient({ queueName: ctx.queueName });

    let result: unknown;
    absurd.registerTask({ name: "no-headers" }, (_params, taskCtx) => {
      result = taskCtx.headers["nonexistent"];
      return resolved("done");
    });

    await absurd.spawn("no-headers", {});
    await absurd.workBatch("worker1", 60, 1);

    assert.equal(result, undefined);
  });
});

Deno.test("hooks: headers getter returns empty object when none set", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    const absurd = ctx.createClient({ queueName: ctx.queueName });

    let result: unknown;
    absurd.registerTask({ name: "empty-headers" }, (_params, taskCtx) => {
      result = taskCtx.headers;
      return resolved("done");
    });

    await absurd.spawn("empty-headers", {});
    await absurd.workBatch("worker1", 60, 1);

    assert.deepEqual(result, {});
  });
});

Deno.test("hooks: headers getter returns all headers", async () => {
  await withTestContext("hooks_queue", async (ctx) => {
    const absurd = ctx.createClient({ queueName: ctx.queueName });

    let result: unknown;
    absurd.registerTask({ name: "all-headers" }, (_params, taskCtx) => {
      result = taskCtx.headers;
      return resolved("done");
    });

    await absurd.spawn(
      "all-headers",
      {},
      {
        headers: { a: 1, b: "two", c: true },
      },
    );
    await absurd.workBatch("worker1", 60, 1);

    assert.deepEqual(result, { a: 1, b: "two", c: true });
  });
});
