<script lang="ts">
  import { goto } from "$app/navigation";
  import { page } from "$app/state";
  import { onMount } from "svelte";
  import Button from "$lib/components/Button.svelte";
  import JsonBlock from "$lib/components/JsonBlock.svelte";
  import SelectField from "$lib/components/SelectField.svelte";
  import { getAbsurdProvider, type TaskRun, type TaskRunFilters } from "$lib/providers/absurdData";

  const provider = getAbsurdProvider();
  const allQueuesLabel = "All queues";
  const allStatusesLabel = "All statuses";
  const allTaskNamesLabel = "All task names";
  const defaultLimit = 500;
  const statusOptions = [
    allStatusesLabel,
    "running",
    "failed",
    "completed",
    "sleeping",
    "pending",
    "cancelled",
  ] as const;
  type StatusOption = (typeof statusOptions)[number];
  let queueOptions = $state<string[]>([]);
  let taskNameOptions = $state<string[]>([allTaskNamesLabel]);
  const urlQueue = $derived(page.url.searchParams.get("queue") ?? allQueuesLabel);
  const urlSearch = $derived(page.url.searchParams.get("q") ?? "");
  let selectedQueue = $state(allQueuesLabel);
  let selectedStatus = $state<StatusOption>(allStatusesLabel);
  let selectedTaskName = $state(allTaskNamesLabel);
  let lastUrlQueue = $state(allQueuesLabel);
  let searchTerm = $state("");
  let activeSearch = $state("");
  let searchDebounce: ReturnType<typeof setTimeout> | null = null;
  let lastUrlSearch = $state("");
  let taskRuns = $state<TaskRun[]>([]);
  let totalCount = $state(0);
  let expandedId = $state<string | null>(null);
  let hoveredTaskId = $state<string | null>(null);
  let isReady = $state(false);
  let isLoading = $state(false);
  let limit = $state(defaultLimit);
  let lastFilterKey = $state("");
  const handleRefresh = () => {
    void refreshTaskRuns(buildFilters());
    void refreshTaskNameOptions(selectedQueue);
  };
  const handleLoadAll = () => {
    limit = 0;
  };
  const toggleExpanded = (runId: string) => {
    expandedId = expandedId === runId ? null : runId;
  };
  const applyTaskNameFilter = (name: string) => {
    selectedTaskName = name;
    if (activeSearch || searchTerm) {
      activeSearch = "";
      searchTerm = "";
      updateQuery({ q: null });
    }
  };
  const applyTaskIdFilter = (taskId: string) => {
    searchTerm = taskId;
    activeSearch = taskId;
    updateQuery({ q: taskId });
  };
  const handleSearchInput = (event: Event) => {
    const target = event.currentTarget as HTMLInputElement | null;
    if (!target) return;
    searchTerm = target.value;
    if (searchDebounce) {
      clearTimeout(searchDebounce);
    }
    searchDebounce = setTimeout(() => {
      const nextSearch = searchTerm.trim();
      activeSearch = nextSearch;
      updateQuery({ q: nextSearch || null });
    }, 200);
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
    if (urlSearch !== lastUrlSearch) {
      lastUrlSearch = urlSearch;
      if (searchTerm !== urlSearch) {
        searchTerm = urlSearch;
      }
      if (activeSearch !== urlSearch) {
        activeSearch = urlSearch;
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

  const buildFilters = (): TaskRunFilters => ({
    queueName: selectedQueue === allQueuesLabel ? undefined : selectedQueue,
    status: selectedStatus === allStatusesLabel ? undefined : selectedStatus,
    taskName: selectedTaskName === allTaskNamesLabel ? undefined : selectedTaskName,
    search: activeSearch.trim() === "" ? undefined : activeSearch,
    limit,
  });

  const refreshTaskRuns = async (filters: TaskRunFilters) => {
    isLoading = true;
    try {
      const page = await provider.getTaskRunsPage(filters);
      taskRuns = page.runs;
      totalCount = page.totalCount;
    } finally {
      isLoading = false;
    }
  };

  const hasMore = $derived(limit > 0 && totalCount > taskRuns.length);
  const refreshTaskNameOptions = async (queueName: string) => {
    const names = await provider.getTaskNameOptions(
      queueName === allQueuesLabel ? undefined : queueName
    );
    taskNameOptions = [allTaskNamesLabel, ...names];
    if (!taskNameOptions.includes(selectedTaskName)) {
      selectedTaskName = allTaskNamesLabel;
    }
  };

  $effect(() => {
    if (!isReady) return;
    const nextKey = `${selectedQueue}|${selectedStatus}|${selectedTaskName}|${activeSearch}`;
    if (nextKey !== lastFilterKey) {
      lastFilterKey = nextKey;
      if (limit !== defaultLimit) {
        limit = defaultLimit;
      }
    }
  });

  $effect(() => {
    if (!isReady) return;
    const filters = buildFilters();
    void refreshTaskRuns(filters);
  });

  $effect(() => {
    if (!isReady) return;
    void refreshTaskNameOptions(selectedQueue);
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
    <Button
      type="button"
      class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700"
      onclick={handleRefresh}
    >
      Refresh
    </Button>
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
        placeholder="Search IDs, names, queue, or params..."
        class="rounded-md border border-black/10 bg-white px-3 py-2 text-sm text-slate-700"
        value={searchTerm}
        oninput={handleSearchInput}
        autocapitalize="off"
        autocorrect="off"
        spellcheck={false}
      />
    </label>
    <SelectField label="Queue" bind:value={selectedQueue}>
      <option>{allQueuesLabel}</option>
      {#each queueOptions as queue}
        <option value={queue}>{queue}</option>
      {/each}
    </SelectField>
    <SelectField label="Status" bind:value={selectedStatus}>
      {#each statusOptions as option}
        <option value={option}>{option}</option>
      {/each}
    </SelectField>
    <SelectField label="Task name" bind:value={selectedTaskName}>
      {#each taskNameOptions as option}
        <option value={option}>{option}</option>
      {/each}
    </SelectField>
  </div>

  <div class="mt-4 flex flex-wrap items-center justify-between gap-2 text-sm text-slate-600">
    <span>
      {#if totalCount === 0}
        Showing 0 tasks
      {:else}
        Showing 1–{taskRuns.length} of {totalCount} task runs
      {/if}
    </span>
    {#if isLoading}
      <span class="inline-flex items-center gap-2 text-xs text-slate-500">
        <span
          class="h-3 w-3 animate-spin rounded-full border border-slate-300 border-t-slate-600"
          aria-hidden="true"
        ></span>
        Loading task runs...
      </span>
    {/if}
  </div>

  <div class="mt-6 overflow-hidden rounded-lg border border-black/10">
    <table class="min-w-full border-collapse text-left text-sm">
      <thead class="bg-slate-100 text-xs font-semibold uppercase tracking-wide text-slate-600">
        <tr>
          <th class="px-4 py-3">Task Run</th>
          <th class="px-4 py-3">Task Name</th>
          <th class="px-4 py-3">Queue</th>
          <th class="px-4 py-3">Status</th>
          <th class="px-4 py-3">Attempt</th>
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
            <td class="px-4 py-3">
              <div class="flex flex-col gap-1">
                <span class="font-mono text-xs text-slate-700">run: {run.runId}</span>
                <button
                  type="button"
                  aria-label="Highlight task id"
                  class={`w-fit cursor-default text-left font-mono text-[10px] ${
                    hoveredTaskId === run.id
                      ? "rounded bg-amber-50 px-1 text-amber-700"
                      : "text-slate-500"
                  }`}
                  onmouseenter={() => {
                    hoveredTaskId = run.id;
                  }}
                  onmouseleave={() => {
                    hoveredTaskId = null;
                  }}
                >
                  task: {run.id}
                </button>
              </div>
            </td>
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
            <td class="px-4 py-3 text-slate-600">{run.age}</td>
            <td class="px-4 py-3 text-slate-600">{expandedId === run.runId ? "▲" : "▼"}</td>
          </tr>
          {#if expandedId === run.runId}
            <tr class="border-t border-black/10 bg-white">
              <td colspan="7" class="px-4 py-4">
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
                        <button
                          type="button"
                          class="cursor-pointer text-left text-slate-700 hover:underline"
                          onclick={() => applyTaskNameFilter(run.name)}
                        >
                          {run.name}
                        </button>
                      </dd>
                    </div>
                    <div class="flex gap-2">
                      <dt class="text-slate-500">Queue:</dt>
                      <dd>{run.queue}</dd>
                    </div>
                    <div class="flex gap-2">
                      <dt class="text-slate-500">Task ID:</dt>
                      <dd>
                        <button
                          type="button"
                          class="cursor-pointer rounded bg-blue-50 px-1 py-0.5 text-left font-mono text-xs text-blue-700 hover:underline"
                          onclick={() => applyTaskIdFilter(run.id)}
                          onmouseenter={() => {
                            hoveredTaskId = run.id;
                          }}
                          onmouseleave={() => {
                            hoveredTaskId = null;
                          }}
                        >
                          {run.id}
                        </button>
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

                <div class="mt-4">
                  <JsonBlock title="Parameters" value={run.paramsJson} emptyText={"{}"} />
                </div>

                <div class="mt-3">
                  <JsonBlock
                    title="Final State"
                    value={run.finalStateJson}
                    emptyText="No final state yet."
                  />
                </div>
              </td>
            </tr>
          {/if}
        {/each}
      </tbody>
    </table>
  </div>
  <div class="mt-4 flex items-center justify-end">
    {#if hasMore}
      <Button
        type="button"
        class="rounded-md border border-black/10 bg-white px-3 py-1.5 text-xs font-medium text-slate-600"
        onclick={handleLoadAll}
        disabled={isLoading}
      >
        Load all
      </Button>
    {/if}
  </div>
</section>
