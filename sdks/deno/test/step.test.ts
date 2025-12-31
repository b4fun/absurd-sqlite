import assert from "node:assert/strict";

import { randomName, withTestContext } from "./setup.ts";
import { assertMatchObject, rejected, resolved, useFakeTime } from "./utils.ts";

Deno.test("step: executes and returns value", async () => {
  await withTestContext("step_queue", async (ctx) => {
    const absurd = ctx.absurd;
    absurd.registerTask<{ value: number }, { result: string }>(
      { name: "basic" },
      async (params, taskCtx) => {
        const result = await taskCtx.step("process", () => {
          return resolved(`processed-${params.value}`);
        });
        return { result };
      },
    );

    const { taskID } = await absurd.spawn("basic", { value: 42 });
    await absurd.workBatch(randomName("w"), 60, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { result: "processed-42" },
    });
  });
});

Deno.test("step: result cached and not re-executed on retry", async () => {
  await withTestContext("step_queue", async (ctx) => {
    const absurd = ctx.absurd;
    let executionCount = 0;
    let attemptCount = 0;

    absurd.registerTask<void, { random: number; count: number }>(
      { name: "cache", defaultMaxAttempts: 2 },
      async (_params, taskCtx) => {
        attemptCount++;

        const cached = await taskCtx.step("generate-random", () => {
          executionCount++;
          return resolved(Math.random());
        });

        if (attemptCount === 1) {
          throw new Error("Intentional failure");
        }

        return { random: cached, count: executionCount };
      },
    );

    const { taskID } = await absurd.spawn("cache", undefined);

    const workerID = randomName("w");
    await absurd.workBatch(workerID, 60, 1);
    assert.equal(executionCount, 1);

    await absurd.workBatch(workerID, 60, 1);
    assert.equal(executionCount, 1);
    assert.equal(attemptCount, 2);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { count: 1 },
      attempts: 2,
    });
  });
});

Deno.test("step: multistep retry only re-executes uncompleted steps", async () => {
  await withTestContext("step_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const executed: string[] = [];
    let attemptCount = 0;

    absurd.registerTask<void, { steps: string[]; attemptNum: number }>(
      { name: "multistep-retry", defaultMaxAttempts: 2 },
      async (_params, taskCtx) => {
        attemptCount++;

        const step1 = await taskCtx.step("step1", () => {
          executed.push("step1");
          return resolved("result1");
        });

        const step2 = await taskCtx.step("step2", () => {
          executed.push("step2");
          return resolved("result2");
        });

        if (attemptCount === 1) {
          throw new Error("Fail before step3");
        }

        const step3 = await taskCtx.step("step3", () => {
          executed.push("step3");
          return resolved("result3");
        });

        return { steps: [step1, step2, step3], attemptNum: attemptCount };
      },
    );

    const { taskID } = await absurd.spawn("multistep-retry", undefined);

    const workerID = randomName("w");
    await absurd.workBatch(workerID, 60, 1);
    assert.deepEqual(executed, ["step1", "step2"]);

    await absurd.workBatch(workerID, 60, 1);
    assert.deepEqual(executed, ["step1", "step2", "step3"]);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: {
        steps: ["result1", "result2", "result3"],
        attemptNum: 2,
      },
      attempts: 2,
    });
  });
});

Deno.test("step: repeated step names work correctly", async () => {
  await withTestContext("step_queue", async (ctx) => {
    const absurd = ctx.absurd;
    absurd.registerTask<void, { results: number[] }>(
      { name: "deduplicate" },
      async (_params, taskCtx) => {
        const results: number[] = [];
        for (let i = 0; i < 3; i++) {
          const result = await taskCtx.step("loop-step", () => {
            return resolved(i * 10);
          });
          results.push(result);
        }
        return { results };
      },
    );

    const { taskID } = await absurd.spawn("deduplicate", undefined);
    await absurd.workBatch(randomName("w"), 60, 1);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { results: [0, 10, 20] },
    });
  });
});

Deno.test("step: failed step does not save checkpoint and re-executes", async () => {
  await withTestContext("step_queue", async (ctx) => {
    const absurd = ctx.absurd;
    let attemptCount = 0;

    absurd.registerTask<void, { result: string }>(
      { name: "fail", defaultMaxAttempts: 2 },
      async (_params, taskCtx) => {
        const result = await taskCtx.step("fail", () => {
          attemptCount++;
          if (attemptCount === 1) {
            return rejected(new Error("Step fails on first attempt"));
          }
          return resolved("success");
        });

        return { result };
      },
    );

    const { taskID } = await absurd.spawn("fail", undefined);

    const workerID = randomName("w");
    await absurd.workBatch(workerID, 60, 1);
    assert.equal(attemptCount, 1);

    await absurd.workBatch(workerID, 60, 1);
    assert.equal(attemptCount, 2);

    assertMatchObject(await ctx.getTask(taskID), {
      state: "completed",
      completed_payload: { result: "success" },
      attempts: 2,
    });
  });
});

Deno.test("step: sleepFor suspends until duration elapses", async () => {
  await withTestContext("step_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const base = new Date("2024-05-05T10:00:00Z");
    const clock = useFakeTime(base);
    try {
      await ctx.setFakeNow(base);

      const durationSeconds = 60;
      absurd.registerTask({ name: "sleep-for" }, async (_params, taskCtx) => {
        await taskCtx.sleepFor("wait-for", durationSeconds);
        return { resumed: true };
      });

      const { taskID, runID } = await absurd.spawn("sleep-for", undefined);
      await absurd.workBatch("worker-sleep", 120, 1);

      const sleepingRun = await ctx.getRun(runID);
      assertMatchObject(sleepingRun, { state: "sleeping" });
      const wakeTime = new Date(base.getTime() + durationSeconds * 1000);
      assert.equal(sleepingRun?.available_at?.getTime(), wakeTime.getTime());

      const resumeTime = new Date(wakeTime.getTime() + 5 * 1000);
      clock.set(resumeTime);
      await ctx.setFakeNow(resumeTime);
      await absurd.workBatch("worker-sleep", 120, 1);

      assertMatchObject(await ctx.getTask(taskID), {
        state: "completed",
        completed_payload: { resumed: true },
      });
    } finally {
      clock.restore();
    }
  });
});

Deno.test("step: sleepUntil checkpoint prevents rescheduling", async () => {
  await withTestContext("step_queue", async (ctx) => {
    const absurd = ctx.absurd;
    const base = new Date("2024-05-06T09:00:00Z");
    const clock = useFakeTime(base);
    try {
      await ctx.setFakeNow(base);

      const wakeTime = new Date(base.getTime() + 5 * 60 * 1000);
      let executions = 0;

      absurd.registerTask({ name: "sleep-until" }, async (_params, taskCtx) => {
        executions++;
        await taskCtx.sleepUntil("sleep-step", wakeTime);
        return { executions };
      });

      const { taskID, runID } = await absurd.spawn("sleep-until", undefined);
      await absurd.workBatch("worker-sleep", 120, 1);

      const checkpointRow = await ctx.getCheckpoint(taskID, "sleep-step");
      assertMatchObject(checkpointRow, {
        checkpoint_name: "sleep-step",
        owner_run_id: runID,
        state: wakeTime.toISOString(),
      });

      const sleepingRun = await ctx.getRun(runID);
      assert.equal(sleepingRun?.state, "sleeping");

      clock.set(wakeTime);
      await ctx.setFakeNow(wakeTime);
      await absurd.workBatch("worker-sleep", 120, 1);

      assertMatchObject(await ctx.getTask(taskID), {
        state: "completed",
        completed_payload: { executions: 2 },
      });
      assert.equal(executions, 2);
    } finally {
      clock.restore();
    }
  });
});
