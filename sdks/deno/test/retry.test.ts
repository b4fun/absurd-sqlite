import assert from "node:assert/strict";

import { randomName, withTestContext } from "./setup.ts";
import { assertMatchObject, rejected, resolved } from "./utils.ts";

Deno.test("retry: fail run without strategy requeues immediately", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;
    let attempts = 0;

    absurd.registerTask(
      { name: "no-strategy", defaultMaxAttempts: 3 },
      () => {
        attempts++;
        if (attempts < 2) {
          return rejected(new Error("boom"));
        }
        return resolved({ attempts });
      },
    );

    const { taskID } = await absurd.spawn("no-strategy", { payload: 1 });

    await absurd.workBatch("worker1", 60, 1);
    assert.equal((await ctx.getTask(taskID))?.state, "pending");
    assert.equal((await ctx.getTask(taskID))?.attempts, 2);

    await absurd.workBatch("worker1", 60, 1);
    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      attempts: 2,
      completed_payload: { attempts: 2 },
    });
  });
});

Deno.test("retry: exponential backoff retry strategy", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const baseTime = new Date("2024-05-01T10:00:00Z");
    await ctx.setFakeNow(baseTime);

    let attempts = 0;

    absurd.registerTask({ name: "exp-backoff" }, () => {
      attempts++;
      if (attempts < 3) {
        return rejected(new Error(`fail-${attempts}`));
      }
      return resolved({ success: true });
    });

    const { taskID } = await absurd.spawn("exp-backoff", undefined, {
      maxAttempts: 3,
      retryStrategy: { kind: "exponential", baseSeconds: 40, factor: 2 },
    });

    await absurd.workBatch("worker1", 60, 1);
    let task = await ctx.getTask(taskID);
    assert.equal(task?.state, "sleeping");
    assert.equal(task?.attempts, 2);

    await ctx.setFakeNow(new Date(baseTime.getTime() + 40 * 1000));

    await absurd.workBatch("worker1", 60, 1);
    task = await ctx.getTask(taskID);
    assert.equal(task?.state, "sleeping");
    assert.equal(task?.attempts, 3);

    await ctx.setFakeNow(new Date(baseTime.getTime() + 40 * 1000 + 80 * 1000));

    await absurd.workBatch("worker1", 60, 1);
    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      attempts: 3,
      completed_payload: { success: true },
    });
  });
});

Deno.test("retry: fixed backoff retry strategy", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const baseTime = new Date("2024-05-01T11:00:00Z");
    await ctx.setFakeNow(baseTime);

    let attempts = 0;

    absurd.registerTask({ name: "fixed-backoff" }, () => {
      attempts++;
      if (attempts < 2) {
        return rejected(new Error("first-fail"));
      }
      return resolved({ attempts });
    });

    const { taskID } = await absurd.spawn("fixed-backoff", undefined, {
      maxAttempts: 2,
      retryStrategy: { kind: "fixed", baseSeconds: 10 },
    });

    await absurd.workBatch("worker1", 60, 1);
    assert.equal((await ctx.getTask(taskID))?.state, "sleeping");

    await ctx.setFakeNow(new Date(baseTime.getTime() + 10 * 1000));

    await absurd.workBatch("worker1", 60, 1);
    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      attempts: 2,
    });
  });
});

Deno.test("retry: task fails permanently after max attempts exhausted", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask(
      { name: "always-fail", defaultMaxAttempts: 2 },
      () => {
        return rejected(new Error("always fails"));
      },
    );

    const { taskID } = await absurd.spawn("always-fail", undefined);

    await absurd.workBatch("worker1", 60, 1);
    assert.equal((await ctx.getTask(taskID))?.state, "pending");

    await absurd.workBatch("worker1", 60, 1);
    assertMatchObject(await ctx.getTask(taskID), {
      state: "failed",
      attempts: 2,
    });
  });
});

Deno.test("retry: cancellation by max duration", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const baseTime = new Date("2024-05-01T09:00:00Z");
    await ctx.setFakeNow(baseTime);

    absurd.registerTask({ name: "duration-cancel" }, () => {
      return rejected(new Error("always fails"));
    });

    const { taskID } = await absurd.spawn("duration-cancel", undefined, {
      maxAttempts: 4,
      retryStrategy: { kind: "fixed", baseSeconds: 30 },
      cancellation: { maxDuration: 90 },
    });

    await absurd.workBatch("worker1", 60, 1);

    await ctx.setFakeNow(new Date(baseTime.getTime() + 91 * 1000));
    await absurd.workBatch("worker1", 60, 1);

    const task = await ctx.getTask(taskID);
    assert.equal(task?.state, "cancelled");
    assert.notEqual(task?.cancelled_at, null);

    const runs = await ctx.getRuns(taskID);
    assert.equal(runs.length, 2);
    assert.equal(runs[1].state, "cancelled");
  });
});

Deno.test("retry: cancellation by max delay", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const baseTime = new Date("2024-05-01T08:00:00Z");
    await ctx.setFakeNow(baseTime);

    absurd.registerTask({ name: "delay-cancel" }, () => {
      return resolved({ done: true });
    });

    const { taskID } = await absurd.spawn("delay-cancel", undefined, {
      cancellation: { maxDelay: 60 },
    });

    await ctx.setFakeNow(new Date(baseTime.getTime() + 61 * 1000));
    await absurd.workBatch("worker1", 60, 1);

    const task = await ctx.getTask(taskID);
    assert.equal(task?.state, "cancelled");
    assert.notEqual(task?.cancelled_at, null);
  });
});

Deno.test("retry: manual cancel pending task", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "pending-cancel" }, () => {
      return resolved({ ok: true });
    });

    const { taskID } = await absurd.spawn("pending-cancel", { data: 1 });

    await absurd.cancelTask(taskID);

    const task = await ctx.getTask(taskID);
    assert.equal(task?.state, "cancelled");
    assert.notEqual(task?.cancelled_at, null);
  });
});

Deno.test("retry: manual cancel running task", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "running-cancel" }, () => {
      return resolved({ ok: true });
    });

    const { taskID } = await absurd.spawn("running-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });
    assert.equal(claim.task_id, taskID);

    await absurd.cancelTask(taskID);

    const task = await ctx.getTask(taskID);
    assert.equal(task?.state, "cancelled");
    assert.notEqual(task?.cancelled_at, null);
  });
});

Deno.test("retry: cancel blocks checkpoint writes", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "checkpoint-cancel" }, () => {
      return resolved({ ok: true });
    });

    const { taskID } = await absurd.spawn("checkpoint-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    await absurd.cancelTask(taskID);

    await ctx.expectCancelledError(
      ctx.setTaskCheckpointState(
        taskID,
        "step-1",
        { result: "value" },
        claim.run_id,
        60,
      ),
    );
  });
});

Deno.test("retry: cancel blocks awaitEvent registrations", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "await-cancel" }, () => {
      return resolved({ ok: true });
    });

    const { taskID } = await absurd.spawn("await-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    await absurd.cancelTask(taskID);

    await ctx.expectCancelledError(
      ctx.awaitEventInternal(
        taskID,
        claim.run_id,
        "wait-step",
        "some-event",
        null,
      ),
    );
  });
});

Deno.test("retry: cancel blocks extendClaim", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "extend-cancel" }, () => {
      return resolved({ ok: true });
    });

    const { taskID } = await absurd.spawn("extend-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    await absurd.cancelTask(taskID);

    await ctx.expectCancelledError(ctx.extendClaim(claim.run_id, 30));
  });
});

Deno.test("retry: cancel is idempotent", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "idempotent-cancel" }, () => {
      return resolved({ ok: true });
    });

    const { taskID } = await absurd.spawn("idempotent-cancel", { data: 1 });
    await absurd.cancelTask(taskID);
    const first = await ctx.getTask(taskID);
    assert.notEqual(first?.cancelled_at, null);

    await absurd.cancelTask(taskID);
    const second = await ctx.getTask(taskID);
    assert.equal(
      second?.cancelled_at?.getTime(),
      first?.cancelled_at?.getTime(),
    );
  });
});

Deno.test("retry: cancelling completed task is a no-op", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask({ name: "complete-cancel" }, () => {
      return resolved({ status: "done" });
    });

    const { taskID } = await absurd.spawn("complete-cancel", { data: 1 });
    await absurd.workBatch("worker-1", 60, 1);

    await absurd.cancelTask(taskID);

    const task = await ctx.getTask(taskID);
    assert.equal(task?.state, "completed");
    assert.equal(task?.cancelled_at, null);
  });
});

Deno.test("retry: cancelling failed task is a no-op", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;

    absurd.registerTask(
      { name: "failed-cancel", defaultMaxAttempts: 1 },
      () => {
        return rejected(new Error("boom"));
      },
    );

    const { taskID } = await absurd.spawn("failed-cancel", { data: 1 });
    await absurd.workBatch("worker-1", 60, 1);

    await absurd.cancelTask(taskID);

    const task = await ctx.getTask(taskID);
    assert.equal(task?.state, "failed");
    assert.equal(task?.cancelled_at, null);
  });
});

Deno.test("retry: cancel sleeping task transitions run to cancelled", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const eventName = randomName("sleep-event");

    absurd.registerTask({ name: "sleep-cancel" }, () => {
      return resolved({ ok: true });
    });

    const { taskID } = await absurd.spawn("sleep-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    await ctx.awaitEventInternal(
      taskID,
      claim.run_id,
      "wait-step",
      eventName,
      300,
    );

    const sleepingTask = await ctx.getTask(taskID);
    assert.equal(sleepingTask?.state, "sleeping");

    await absurd.cancelTask(taskID);

    const cancelledTask = await ctx.getTask(taskID);
    assert.equal(cancelledTask?.state, "cancelled");
    const run = await ctx.getRun(claim.run_id);
    assert.equal(run?.state, "cancelled");
  });
});

Deno.test("retry: cancel non-existent task errors", async () => {
  await withTestContext("retry_queue", async (ctx) => {
    const absurd = ctx.absurd;
    await assert.rejects(
      () => absurd.cancelTask("019a32d3-8425-7ae2-a5af-2f17a6707666"),
      /task not found/i,
    );
  });
});
