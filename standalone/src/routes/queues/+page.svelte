<script lang="ts">
  import { onMount } from "svelte";
  import Button from "$lib/components/Button.svelte";
  import {
    getAbsurdProvider,
    type CleanupTarget,
    type QueueSummary,
  } from "$lib/providers/absurdData";

  const provider = getAbsurdProvider();
  let queueSummaries = $state<QueueSummary[]>([]);
  let isCreating = $state(false);
  let isCreateOpen = $state(false);
  let newQueueName = $state("");
  let createError = $state<string | null>(null);
  let isCleanupOpen = $state(false);
  let cleanupQueue = $state<QueueSummary | null>(null);
  let cleanupTargets = $state<Record<CleanupTarget, boolean>>({
    tasks: true,
    events: false,
  });
  let cleanupTaskAge = $state<"7d" | "30d" | "all">("7d");
  let cleanupEventAge = $state<"7d" | "30d" | "all">("7d");
  let cleanupError = $state<string | null>(null);
  let isCleaning = $state(false);

  const cleanupTtlSeconds = {
    "7d": 7 * 24 * 60 * 60,
    "30d": 30 * 24 * 60 * 60,
    all: 0,
  } as const;
  const refreshData = async () => {
    queueSummaries = await provider.getQueueSummaries();
  };

  const handleRefresh = () => {
    void refreshData();
  };

  const openCreateQueue = () => {
    newQueueName = "";
    createError = null;
    isCreateOpen = true;
  };

  const closeCreateQueue = () => {
    if (isCreating) return;
    isCreateOpen = false;
  };

  const openCleanup = (queue: QueueSummary) => {
    cleanupQueue = queue;
    cleanupTargets = { tasks: true, events: false };
    cleanupTaskAge = "7d";
    cleanupEventAge = "7d";
    cleanupError = null;
    isCleanupOpen = true;
  };

  const closeCleanup = () => {
    if (isCleaning) return;
    isCleanupOpen = false;
    cleanupQueue = null;
  };

  const handleCreateQueue = async (event?: SubmitEvent) => {
    event?.preventDefault();
    if (isCreating) return;
    const trimmedName = newQueueName.trim();
    if (!trimmedName) {
      createError = "Queue name is required.";
      return;
    }

    isCreating = true;
    createError = null;
    try {
      await provider.createQueue(trimmedName);
      await refreshData();
      isCreateOpen = false;
    } catch (error) {
      console.error("Failed to create queue", error);
      createError = "Failed to create queue.";
    } finally {
      isCreating = false;
    }
  };

  const handleCleanup = async (event?: SubmitEvent) => {
    event?.preventDefault();
    if (isCleaning || !cleanupQueue) return;

    if (!cleanupTargets.tasks && !cleanupTargets.events) {
      cleanupError = "Select at least one target.";
      return;
    }

    isCleaning = true;
    cleanupError = null;
    try {
      if (cleanupTargets.tasks) {
        await provider.cleanupQueue({
          queueName: cleanupQueue.name,
          target: "tasks",
          ttlSeconds: cleanupTtlSeconds[cleanupTaskAge],
        });
      }
      if (cleanupTargets.events) {
        await provider.cleanupQueue({
          queueName: cleanupQueue.name,
          target: "events",
          ttlSeconds: cleanupTtlSeconds[cleanupEventAge],
        });
      }
      await refreshData();
      isCleanupOpen = false;
      cleanupQueue = null;
    } catch (error) {
      console.error("Failed to clean up queue", error);
      cleanupError = "Failed to clean up queue.";
    } finally {
      isCleaning = false;
    }
  };

  onMount(() => {
    void refreshData();
  });
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Queues</h1>
    <p class="mt-1 text-sm text-slate-600">
      Inspect queue health, backlog contents, and recent events.
    </p>
  </div>
  <div class="flex items-center gap-3">
    <Button
      type="button"
      class="rounded-md border border-black/10 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
      onclick={openCreateQueue}
      disabled={isCreating}
    >
      New queue
    </Button>
    <Button
      type="button"
      class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700"
      onclick={handleRefresh}
    >
      Refresh
    </Button>
  </div>
</section>

{#if isCreateOpen}
  <div
    class="fixed inset-0 z-10 flex items-center justify-center bg-slate-900/30 p-4"
    role="dialog"
    aria-modal="true"
    aria-label="Create queue"
  >
    <form class="w-full max-w-md rounded-xl bg-white p-6 shadow-xl" onsubmit={handleCreateQueue}>
      <h2 class="text-lg font-semibold text-slate-900">Create new queue</h2>
      <p class="mt-1 text-sm text-slate-500">Queues are created immediately and show up below.</p>
      <label class="mt-4 flex flex-col gap-2 text-sm font-medium text-slate-700">
        Queue name
        <input
          type="text"
          bind:value={newQueueName}
          placeholder="e.g. billing-webhooks"
          autocorrect="off"
          autocomplete="off"
          autocapitalize="off"
          class="rounded-md border border-black/10 bg-white px-3 py-2 text-sm text-slate-700"
        />
      </label>
      {#if createError}
        <p class="mt-2 text-sm text-rose-600">{createError}</p>
      {/if}
      <div class="mt-6 flex items-center justify-end gap-3">
        <Button
          type="button"
          class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm text-slate-700"
          onclick={closeCreateQueue}
          disabled={isCreating}
        >
          Cancel
        </Button>
        <Button
          type="submit"
          class="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
          disabled={isCreating}
        >
          {isCreating ? "Creating..." : "Create queue"}
        </Button>
      </div>
    </form>
  </div>
{/if}

{#if isCleanupOpen}
  <div
    class="fixed inset-0 z-10 flex items-center justify-center bg-slate-900/30 p-4"
    role="dialog"
    aria-modal="true"
    aria-label="Clean up queue"
  >
    <form class="w-full max-w-3xl rounded-xl bg-white p-6 shadow-xl" onsubmit={handleCleanup}>
      <h2 class="text-lg font-semibold text-slate-900">
        Clean up {cleanupQueue?.name}
      </h2>
      <p class="mt-1 text-sm text-slate-500">
        Remove old terminal tasks or events from this queue.
      </p>

      <div class="mt-5 space-y-4">
        <div class="rounded-lg border border-black/10 bg-white p-4">
          <div class="space-y-4">
            <label class="flex items-center gap-2 text-sm font-medium text-slate-700 cursor-pointer">
              <input type="checkbox" bind:checked={cleanupTargets.tasks} class="cursor-pointer" />
              Tasks
            </label>
            <div class="flex flex-col gap-2 text-sm font-medium text-slate-700">
              Age
              <div class="flex flex-wrap gap-2">
                <button
                  type="button"
                  class={`rounded-md border px-3 py-1.5 text-sm cursor-pointer disabled:cursor-not-allowed ${
                    cleanupTaskAge === "7d"
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-black/10 bg-white text-slate-700"
                  } ${!cleanupTargets.tasks ? "opacity-50" : ""}`}
                  onclick={() => cleanupTargets.tasks && (cleanupTaskAge = "7d")}
                  disabled={!cleanupTargets.tasks}
                >
                  &gt; 7d
                </button>
                <button
                  type="button"
                  class={`rounded-md border px-3 py-1.5 text-sm cursor-pointer disabled:cursor-not-allowed ${
                    cleanupTaskAge === "30d"
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-black/10 bg-white text-slate-700"
                  } ${!cleanupTargets.tasks ? "opacity-50" : ""}`}
                  onclick={() => cleanupTargets.tasks && (cleanupTaskAge = "30d")}
                  disabled={!cleanupTargets.tasks}
                >
                  &gt; 30d
                </button>
                <button
                  type="button"
                  class={`rounded-md border px-3 py-1.5 text-sm cursor-pointer disabled:cursor-not-allowed ${
                    cleanupTaskAge === "all"
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-black/10 bg-white text-slate-700"
                  } ${!cleanupTargets.tasks ? "opacity-50" : ""}`}
                  onclick={() => cleanupTargets.tasks && (cleanupTaskAge = "all")}
                  disabled={!cleanupTargets.tasks}
                >
                  All
                </button>
              </div>
            </div>
          </div>
          <p class="mt-3 text-xs text-slate-500">
            Pending, running, and sleeping tasks will be preserved.
          </p>
        </div>

        <div class="rounded-lg border border-black/10 bg-white p-4">
          <div class="space-y-4">
            <label class="flex items-center gap-2 text-sm font-medium text-slate-700 cursor-pointer">
              <input type="checkbox" bind:checked={cleanupTargets.events} class="cursor-pointer" />
              Events
            </label>
            <div class="flex flex-col gap-2 text-sm font-medium text-slate-700">
              Age
              <div class="flex flex-wrap gap-2">
                <button
                  type="button"
                  class={`rounded-md border px-3 py-1.5 text-sm cursor-pointer disabled:cursor-not-allowed ${
                    cleanupEventAge === "7d"
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-black/10 bg-white text-slate-700"
                  } ${!cleanupTargets.events ? "opacity-50" : ""}`}
                  onclick={() => cleanupTargets.events && (cleanupEventAge = "7d")}
                  disabled={!cleanupTargets.events}
                >
                  &gt; 7d
                </button>
                <button
                  type="button"
                  class={`rounded-md border px-3 py-1.5 text-sm cursor-pointer disabled:cursor-not-allowed ${
                    cleanupEventAge === "30d"
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-black/10 bg-white text-slate-700"
                  } ${!cleanupTargets.events ? "opacity-50" : ""}`}
                  onclick={() => cleanupTargets.events && (cleanupEventAge = "30d")}
                  disabled={!cleanupTargets.events}
                >
                  &gt; 30d
                </button>
                <button
                  type="button"
                  class={`rounded-md border px-3 py-1.5 text-sm cursor-pointer disabled:cursor-not-allowed ${
                    cleanupEventAge === "all"
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-black/10 bg-white text-slate-700"
                  } ${!cleanupTargets.events ? "opacity-50" : ""}`}
                  onclick={() => cleanupTargets.events && (cleanupEventAge = "all")}
                  disabled={!cleanupTargets.events}
                >
                  All
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      {#if cleanupError}
        <p class="mt-3 text-sm text-rose-600">{cleanupError}</p>
      {/if}

      <div class="mt-6 flex items-center justify-end gap-3">
        <Button
          type="button"
          class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm text-slate-700"
          onclick={closeCleanup}
          disabled={isCleaning}
        >
          Cancel
        </Button>
        <Button
          type="submit"
          class="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
          disabled={isCleaning}
        >
          {isCleaning ? "Cleaning..." : "Clean up"}
        </Button>
      </div>
    </form>
  </div>
{/if}

<section class="mt-8 space-y-6">
  {#each queueSummaries as queue}
    <article class="rounded-lg border border-black/10 bg-white p-6">
      <div class="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div class="flex items-center gap-3">
            <h2 class="text-2xl font-semibold text-slate-900">{queue.name}</h2>
            <span class="text-sm text-slate-500">{queue.age}</span>
          </div>
          <p class="mt-1 text-sm text-slate-500">{queue.createdAt}</p>
        </div>
        <div class="flex items-center gap-3">
          <a
            href={`/tasks?queue=${queue.name}`}
            class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm text-slate-700"
          >
            Tasks →
          </a>
          <a
            href={`/events?queue=${queue.name}`}
            class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm text-slate-700"
          >
            Events →
          </a>
          <Button
            type="button"
            class="rounded-md border border-black/10 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
            onclick={() => openCleanup(queue)}
          >
            Clean Up
          </Button>
        </div>
      </div>

      <div class="mt-6 grid gap-3 sm:grid-cols-2 lg:grid-cols-6">
        {#each queue.stats as stat}
          <div class="rounded-lg border border-black/10 bg-white p-4 text-center">
            <p class="text-sm text-slate-500">{stat.label}</p>
            <p class="mt-2 text-2xl font-semibold text-slate-900">{stat.value}</p>
          </div>
        {/each}
      </div>
    </article>
  {/each}
</section>
