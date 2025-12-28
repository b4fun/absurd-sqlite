<script lang="ts">
  import { goto } from "$app/navigation";
  import { page } from "$app/state";
  import { onMount } from "svelte";
  import SelectField from "$lib/components/SelectField.svelte";
  import { getAbsurdProvider, type TaskRun } from "$lib/providers/absurdData";

  const provider = getAbsurdProvider();
  const allQueuesLabel = "All queues";
  let queueOptions = $state<string[]>([]);
  const urlQueue = $derived(page.url.searchParams.get("queue") ?? allQueuesLabel);
  let selectedQueue = $state(urlQueue);
  let lastUrlQueue = $state(urlQueue);
  let taskRuns = $state<TaskRun[]>([]);
  let expandedId = $state<string | null>(null);
  let isReady = $state(false);
  const handleRefresh = () => {
    void refreshTaskRuns();
  };
  const toggleExpanded = (runId: string) => {
    expandedId = expandedId === runId ? null : runId;
  };
  const currentStatusByTaskId = $derived(
    taskRuns.reduce<Record<string, { status: TaskRun["status"]; attemptNumber: number }>>(
      (acc, run) => {
        const existing = acc[run.id];
        if (!existing || run.attemptNumber > existing.attemptNumber) {
          acc[run.id] = { status: run.status, attemptNumber: run.attemptNumber };
        }
        return acc;
      },
      {}
    )
  );

  const updateQuery = (updates: Record<string, string | null>) => {
    const url = new URL(page.url);
    Object.entries(updates).forEach(([key, value]) => {
      if (!value) {
        url.searchParams.delete(key);
      } else {
        url.searchParams.set(key, value);
      }
    });
    const nextUrl = `${url.pathname}${url.searchParams.toString() ? `?${url.searchParams}` : ""}`;
    if (nextUrl !== `${page.url.pathname}${page.url.search}`) {
      goto(nextUrl, { replaceState: true, keepFocus: true, noScroll: true });
    }
  };

  $effect(() => {
    if (urlQueue !== lastUrlQueue) {
      lastUrlQueue = urlQueue;
      if (selectedQueue !== urlQueue) {
        selectedQueue = urlQueue;
      }
    }
  });

  $effect(() => {
    updateQuery({ queue: selectedQueue === allQueuesLabel ? null : selectedQueue });
  });

  const statusStyles: Record<TaskRun["status"], string> = {
    running: "border-emerald-200 bg-emerald-50 text-emerald-700",
    failed: "border-rose-200 bg-rose-50 text-rose-700",
    completed: "border-slate-200 bg-slate-100 text-slate-700",
    sleeping: "border-amber-200 bg-amber-50 text-amber-700",
    pending: "border-sky-200 bg-sky-50 text-sky-700",
    cancelled: "border-zinc-200 bg-zinc-50 text-zinc-600",
  };

  const refreshTaskRuns = async () => {
    taskRuns =
      selectedQueue === allQueuesLabel
        ? await provider.getTaskRuns()
        : await provider.getTaskRunsForQueue(selectedQueue);
  };

  $effect(() => {
    if (!isReady) return;
    void refreshTaskRuns();
  });

  onMount(async () => {
    queueOptions = await provider.getQueueNames();
    isReady = true;
  });
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Tasks</h1>
    <p class="mt-1 text-sm text-slate-600">
      Monitor and manage durable tasks across all queues.
    </p>
  </div>
  <div class="flex items-center gap-3">
    <label class="flex items-center gap-2 text-sm text-slate-600">
      <input type="checkbox" checked class="h-4 w-4" />
      <span>Auto-refresh (15s)</span>
    </label>
    <button
      type="button"
      class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:cursor-pointer"
      onclick={handleRefresh}
    >
      Refresh
    </button>
  </div>
</section>

<section class="mt-8 rounded-lg border border-black/10 bg-white p-6">
  <div>
    <h2 class="text-2xl font-semibold text-slate-900">Task Runs</h2>
    <p class="mt-1 text-sm text-slate-500">
      Each row represents a single run. Click a run to view details or open the full task history.
    </p>
  </div>

  <div class="mt-6 grid gap-4 lg:grid-cols-[2fr_repeat(3,_1fr)]">
    <label class="flex flex-col gap-2 text-sm font-medium text-slate-600">
      Search
      <input
        type="search"
        placeholder="Search IDs, names, queue, or params... (Enter to search)"
        class="rounded-md border border-black/10 bg-white px-3 py-2 text-sm text-slate-700"
      />
    </label>
    <SelectField label="Queue" bind:value={selectedQueue}>
      <option>{allQueuesLabel}</option>
      {#each queueOptions as queue}
        <option value={queue}>{queue}</option>
      {/each}
    </SelectField>
    <SelectField label="Status">
      <option>All statuses</option>
    </SelectField>
    <SelectField label="Task name">
      <option>All task names</option>
    </SelectField>
  </div>

  <div class="mt-4 text-sm text-slate-600">
    Showing 1–{taskRuns.length} of {taskRuns.length} tasks
  </div>

  <div class="mt-6 overflow-hidden rounded-lg border border-black/10">
    <table class="min-w-full border-collapse text-left text-sm">
      <thead class="bg-slate-100 text-xs font-semibold uppercase tracking-wide text-slate-600">
        <tr>
          <th class="px-4 py-3">Task ID</th>
          <th class="px-4 py-3">Task Name</th>
          <th class="px-4 py-3">Queue</th>
          <th class="px-4 py-3">Status</th>
          <th class="px-4 py-3">Attempt</th>
          <th class="px-4 py-3">Run ID</th>
          <th class="px-4 py-3">Age</th>
          <th class="px-4 py-3"></th>
        </tr>
      </thead>
      <tbody class="bg-white">
        {#each taskRuns as run}
          <tr
            class="border-t border-black/5 hover:bg-slate-50 hover:cursor-pointer"
            onclick={() => toggleExpanded(run.runId)}
          >
            <td class="px-4 py-3 font-mono text-xs text-slate-600">{run.id}</td>
            <td class="px-4 py-3 text-slate-800">{run.name}</td>
            <td class="px-4 py-3 text-slate-600">{run.queue}</td>
            <td class="px-4 py-3">
              <span
                class={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${statusStyles[run.status]}`}
              >
                {run.status}
              </span>
            </td>
            <td class="px-4 py-3 text-slate-600">{run.attempt}</td>
            <td class="px-4 py-3 font-mono text-xs text-slate-600">{run.runId}</td>
            <td class="px-4 py-3 text-slate-600">{run.age}</td>
            <td class="px-4 py-3 text-slate-600">{expandedId === run.runId ? "▲" : "▼"}</td>
          </tr>
          {#if expandedId === run.runId}
            <tr class="border-t border-black/10 bg-white">
              <td colspan="8" class="px-4 py-4">
                <div class="mt-4">
                  <div class="flex items-center justify-between">
                    <h3 class="text-sm font-semibold text-slate-800">Basic Information</h3>
                    <a
                      href={`/tasks/${run.id}`}
                      class="rounded-md border border-black/10 bg-white px-3 py-1 text-xs text-slate-600 shadow-sm hover:text-slate-900"
                    >
                      View task history
                    </a>
                  </div>
                  <dl class="mt-3 grid gap-2 text-sm text-slate-700 md:grid-cols-2">
                    <div class="flex gap-2">
                      <dt class="text-slate-500">Current status:</dt>
                      <dd>
                        <span
                          class={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${
                            statusStyles[currentStatusByTaskId[run.id]?.status ?? run.status]
                          }`}
                        >
                          {currentStatusByTaskId[run.id]?.status ?? run.status}
                        </span>
                      </dd>
                    </div>
                    <div class="flex gap-2">
                      <dt class="text-slate-500">Task Name:</dt>
                      <dd>
                        <a href={`/tasks/${run.id}`} class="text-slate-700 hover:underline">
                          {run.name}
                        </a>
                      </dd>
                    </div>
                    <div class="flex gap-2">
                      <dt class="text-slate-500">Queue:</dt>
                      <dd>{run.queue}</dd>
                    </div>
                    <div class="flex gap-2">
                      <dt class="text-slate-500">Task ID:</dt>
                      <dd>
                        <a
                          href={`/tasks/${run.id}`}
                          class="rounded bg-blue-50 px-1 py-0.5 font-mono text-xs text-blue-700 hover:underline"
                        >
                          {run.id}
                        </a>
                      </dd>
                    </div>
                    <div class="flex gap-2">
                      <dt class="text-slate-500">Run ID:</dt>
                      <dd class="rounded bg-blue-50 px-1 py-0.5 font-mono text-xs text-blue-700">
                        {run.runId}
                      </dd>
                    </div>
                    <div class="flex gap-2">
                      <dt class="text-slate-500">Worker:</dt>
                      <dd>{run.worker}</dd>
                    </div>
                  </dl>
                </div>

                <div class="mt-4 rounded-md border border-black/10">
                  <div class="flex items-center justify-between border-b border-black/10 bg-slate-50 px-3 py-2 text-xs text-slate-500">
                    <span>Parameters</span>
                    <button type="button" class="hover:text-slate-700 hover:cursor-pointer">
                      Copy
                    </button>
                  </div>
                  <pre class="whitespace-pre-wrap bg-white px-3 py-3 font-mono text-xs text-slate-700">
{run.paramsJson || "{}"}
                  </pre>
                </div>

                <div class="mt-3 rounded-md border border-black/10">
                  <div class="flex items-center justify-between border-b border-black/10 bg-slate-50 px-3 py-2 text-xs text-slate-500">
                    <span>Final State</span>
                    <button type="button" class="hover:text-slate-700 hover:cursor-pointer">
                      Copy
                    </button>
                  </div>
                  <pre class="whitespace-pre-wrap bg-white px-3 py-3 font-mono text-xs text-slate-700">
▼
{run.finalStateJson || "{}"}
                  </pre>
                </div>
              </td>
            </tr>
          {/if}
        {/each}
      </tbody>
    </table>
  </div>
</section>
