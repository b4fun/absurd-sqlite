import assert from "node:assert/strict";

import { randomName, withTestContext } from "./setup.ts";
import { resolved } from "./utils.ts";

Deno.test("idempotent: creates task when idempotencyKey is new", async () => {
  await withTestContext("idempotent", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "idempotent-task" }, () => {
      return resolved({ done: true });
    });

    const result = await absurd.spawn(
      "idempotent-task",
      { value: 42 },
      { idempotencyKey: "unique-key-1" },
    );

    assert(result.taskID);
    assert(result.runID);
    assert.equal(result.attempt, 1);
    assert.equal(Boolean(result.created), true);

    const task = await ctx.getTask(result.taskID);
    assert(task);
    assert.equal(task?.state, "pending");
  });
});

Deno.test("idempotent: returns existing task when key exists", async () => {
  await withTestContext("idempotent", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "dup-task" }, () => {
      return resolved({ done: true });
    });

    const first = await absurd.spawn(
      "dup-task",
      { value: 1 },
      { idempotencyKey: "dup-key" },
    );

    assert(first.taskID);
    assert(first.runID);
    assert.equal(first.attempt, 1);
    assert.equal(Boolean(first.created), true);

    const second = await absurd.spawn(
      "dup-task",
      { value: 2 },
      { idempotencyKey: "dup-key" },
    );

    assert.equal(second.taskID, first.taskID);
    assert.equal(second.runID, first.runID);
    assert.equal(second.attempt, first.attempt);
    assert.equal(Boolean(second.created), false);
  });
});

Deno.test("idempotent: spawn without key always creates new task", async () => {
  await withTestContext("idempotent", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "no-idem-task" }, () => {
      return resolved({ done: true });
    });

    const first = await absurd.spawn("no-idem-task", { value: 1 });
    const second = await absurd.spawn("no-idem-task", { value: 2 });

    assert.notEqual(first.taskID, second.taskID);
    assert(first.runID);
    assert(second.runID);
    assert.equal(Boolean(first.created), true);
    assert.equal(Boolean(second.created), true);
  });
});

Deno.test("idempotent: different keys create separate tasks", async () => {
  await withTestContext("idempotent", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "diff-keys-task" }, () => {
      return resolved({ done: true });
    });

    const first = await absurd.spawn(
      "diff-keys-task",
      { value: 1 },
      { idempotencyKey: "key-a" },
    );
    const second = await absurd.spawn(
      "diff-keys-task",
      { value: 2 },
      { idempotencyKey: "key-b" },
    );

    assert.notEqual(first.taskID, second.taskID);
    assert(first.runID);
    assert(second.runID);
    assert.equal(Boolean(first.created), true);
    assert.equal(Boolean(second.created), true);
  });
});

Deno.test("idempotent: key persists after completion", async () => {
  await withTestContext("idempotent", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "complete-idem-task" }, () => {
      return resolved({ result: "done" });
    });

    const first = await absurd.spawn(
      "complete-idem-task",
      { value: 1 },
      { idempotencyKey: "complete-key" },
    );

    await absurd.workBatch("test-worker", 60, 1);

    const task = await ctx.getTask(first.taskID);
    assert.equal(task?.state, "completed");

    const second = await absurd.spawn(
      "complete-idem-task",
      { value: 2 },
      { idempotencyKey: "complete-key" },
    );

    assert.equal(second.taskID, first.taskID);
    assert.equal(second.runID, first.runID);
    assert.equal(second.attempt, first.attempt);
    assert.equal(Boolean(second.created), false);
  });
});

Deno.test("idempotent: idempotencyKey is scoped to queue", async () => {
  await withTestContext("idempotent", async (ctx) => {
    const absurd = ctx.absurd;
    const otherQueueName = randomName("other_queue");
    await absurd.createQueue(otherQueueName);

    absurd.registerTask(
      { name: "queue-scoped-task", queue: ctx.queueName },
      () => resolved({ done: true }),
    );
    absurd.registerTask(
      { name: "queue-scoped-task-other", queue: otherQueueName },
      () => resolved({ done: true }),
    );

    const first = await absurd.spawn(
      "queue-scoped-task",
      { value: 1 },
      { idempotencyKey: "scoped-key" },
    );

    const second = await absurd.spawn(
      "queue-scoped-task-other",
      { value: 2 },
      { idempotencyKey: "scoped-key" },
    );

    assert.notEqual(first.taskID, second.taskID);
    assert.equal(Boolean(first.created), true);
    assert.equal(Boolean(second.created), true);

    await absurd.dropQueue(otherQueueName);
  });
});

Deno.test("idempotent: fire-and-forget patterns", async () => {
  await withTestContext("idempotent", async (ctx) => {
    const absurd = ctx.absurd;
    let execCount = 0;
    absurd.registerTask({ name: "fire-forget-task" }, () => {
      execCount++;
      return resolved({ done: true });
    });

    await absurd.spawn(
      "fire-forget-task",
      {},
      { idempotencyKey: "daily-report:2025-01-15" },
    );
    await absurd.spawn(
      "fire-forget-task",
      {},
      { idempotencyKey: "daily-report:2025-01-15" },
    );
    await absurd.spawn(
      "fire-forget-task",
      {},
      { idempotencyKey: "daily-report:2025-01-15" },
    );

    await absurd.workBatch("test-worker", 60, 10);

    assert.equal(execCount, 1);
  });
});
