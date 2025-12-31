<script lang="ts">
  import { onMount } from "svelte";
  import Button from "$lib/components/Button.svelte";
  import {
    getAbsurdProvider,
    type OverviewMetrics,
    type QueueMetric,
  } from "$lib/providers/absurdData";

  const provider = getAbsurdProvider();
  const overviewDefaults: OverviewMetrics = {
    activeQueues: 0,
    messagesProcessed: 0,
    messagesInQueue: 0,
    visibleNow: 0,
  };

  let overview = $state<OverviewMetrics | null>(null);
  let queueMetrics = $state<QueueMetric[]>([]);
  const overviewData = $derived(overview ?? overviewDefaults);

  const refreshData = async () => {
    overview = await provider.getOverviewMetrics();
    queueMetrics = await provider.getQueueMetrics();
  };

  const handleRefresh = () => {
    void refreshData();
  };

  const statCards = $derived([
    {
      label: "Active queues",
      value: overviewData.activeQueues,
      description: "Queues with registered metrics",
    },
    {
      label: "Messages processed",
      value: overviewData.messagesProcessed,
      description: "Total messages the queues have seen",
    },
    {
      label: "Messages in queue",
      value: overviewData.messagesInQueue,
      description: "Unclaimed messages in queue storage",
    },
    {
      label: "Visible right now",
      value: overviewData.visibleNow,
      description: "Ready for immediate consumption",
    },
  ]);

  onMount(() => {
    void refreshData();
  });
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Overview</h1>
    <p class="mt-1 text-sm text-slate-600">
      Current queue health across your Absurd installation.
    </p>
  </div>
  <Button
    type="button"
    class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700"
    onclick={handleRefresh}
  >
    Refresh
  </Button>
</section>

<section class="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
  {#each statCards as card}
    <div class="rounded-lg border border-black/10 bg-white p-4">
      <p class="text-sm font-medium text-slate-700">{card.label}</p>
      <p class="mt-3 text-3xl font-semibold text-slate-900">{card.value}</p>
      <p class="mt-2 text-sm text-slate-500">{card.description}</p>
    </div>
  {/each}
</section>

<section class="mt-8 rounded-lg border border-black/10 bg-white p-6">
  <div>
    <h2 class="text-2xl font-semibold text-slate-900">Queue metrics</h2>
    <p class="mt-1 text-sm text-slate-500">
      Visibility into backlog depth and message freshness.
    </p>
  </div>

  <div class="mt-6 overflow-hidden rounded-lg border border-black/10">
    <table class="min-w-full border-collapse text-left text-sm">
      <thead class="bg-slate-100 text-xs font-semibold uppercase tracking-wide text-slate-600">
        <tr>
          <th class="px-4 py-3">Queue</th>
          <th class="px-4 py-3">In Queue</th>
          <th class="px-4 py-3">Visible</th>
          <th class="px-4 py-3">Newest age</th>
          <th class="px-4 py-3">Oldest age</th>
          <th class="px-4 py-3">Total seen</th>
          <th class="px-4 py-3">Scraped</th>
          <th class="px-4 py-3">Actions</th>
        </tr>
      </thead>
      <tbody class="bg-white">
        {#each queueMetrics as metric}
          <tr class="border-t border-black/5">
            <td class="px-4 py-3 font-medium text-slate-900">{metric.name}</td>
            <td class="px-4 py-3 text-slate-600">{metric.inQueue}</td>
            <td class="px-4 py-3 text-slate-600">{metric.visible}</td>
            <td class="px-4 py-3 text-slate-600">{metric.newestAge}</td>
            <td class="px-4 py-3 text-slate-600">{metric.oldestAge}</td>
            <td class="px-4 py-3 text-slate-600">{metric.totalSeen}</td>
            <td class="px-4 py-3 text-slate-600">{metric.scrapedAt}</td>
            <td class="px-4 py-3 text-slate-600">
              <div class="flex items-center gap-2">
                <a class="text-sm text-slate-700" href={`/tasks?queue=${metric.name}`}>
                  Tasks →
                </a>
                <a class="text-sm text-slate-700" href={`/events?queue=${metric.name}`}>
                  Events →
                </a>
              </div>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</section>
