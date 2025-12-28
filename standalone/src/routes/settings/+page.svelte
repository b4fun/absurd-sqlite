<script lang="ts">
  import { onMount } from "svelte";
  import {
    getAbsurdProvider,
    type MigrationEntry,
    type SettingsInfo,
  } from "$lib/providers/absurdData";

  const provider = getAbsurdProvider();
  const defaults: SettingsInfo = {
    absurdVersion: "--",
    sqliteVersion: "--",
    dbPath: "--",
    migration: {
      status: "missing",
      appliedCount: 0,
      latestVersion: null,
      latestAppliedAt: null,
    },
  };

  let settings = $state<SettingsInfo | null>(null);
  let migrations = $state<MigrationEntry[]>([]);
  const data = $derived(settings ?? defaults);
  const statusLabel = $derived(
    data.migration.status === "applied" ? "Up to date" : "Not applied",
  );
  const statusClasses = $derived(
    data.migration.status === "applied"
      ? "bg-emerald-100 text-emerald-700"
      : "bg-amber-100 text-amber-700",
  );
  const hasPendingMigrations = $derived(
    migrations.some((migration) => migration.status === "pending"),
  );

  const refreshData = async () => {
    settings = await provider.getSettingsInfo();
    migrations = await provider.getMigrations();
  };

  const handleRefresh = () => {
    void refreshData();
  };

  const handleApplyAll = async () => {
    await provider.applyMigrationsAll();
    await refreshData();
  };

  const handleApplyMigration = async (migrationId: number) => {
    await provider.applyMigration(migrationId);
    await refreshData();
  };

  onMount(() => {
    void refreshData();
  });
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Settings</h1>
    <p class="mt-1 text-sm text-slate-600">
      Version details, storage location, and migration health.
    </p>
  </div>
  <button
    type="button"
    class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:cursor-pointer"
    onclick={handleRefresh}
  >
    Refresh
  </button>
</section>

<section class="mt-8 grid gap-4 grid-cols-2">
  <article class="rounded-lg border border-black/10 bg-white p-6">
    <h2 class="text-lg font-semibold text-slate-900">Version</h2>
    <p class="mt-1 text-sm text-slate-500">Runtime and extension versions.</p>
    <dl class="mt-4 space-y-3 text-sm">
      <div class="flex items-center justify-between gap-4">
        <dt class="text-slate-500">Absurd</dt>
        <dd class="text-right font-medium text-slate-900">{data.absurdVersion}</dd>
      </div>
      <div class="flex items-center justify-between gap-4">
        <dt class="text-slate-500">SQLite</dt>
        <dd class="text-right font-medium text-slate-900">{data.sqliteVersion}</dd>
      </div>
    </dl>
  </article>

  <article class="rounded-lg border border-black/10 bg-white p-6">
    <h2 class="text-lg font-semibold text-slate-900">Database</h2>
    <p class="mt-1 text-sm text-slate-500">Primary storage location for task data.</p>
    <dl class="mt-4 space-y-3 text-sm">
      <div class="space-y-2">
        <dt class="text-slate-500">File path</dt>
        <dd class="break-all rounded-md border border-black/10 bg-slate-50 px-3 py-2 font-mono text-xs text-slate-700">
          {data.dbPath}
        </dd>
      </div>
    </dl>
  </article>

  <article class="rounded-lg border border-black/10 bg-white p-6 col-span-2">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div>
        <h2 class="text-2xl font-semibold text-slate-900">Migrations</h2>
        <p class="mt-1 text-sm text-slate-500">
          Schema upgrade status and migration history.
        </p>
      </div>
      <div class="flex items-center gap-3">
        <button
          type="button"
          class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:cursor-pointer disabled:cursor-not-allowed disabled:opacity-60"
          disabled={!hasPendingMigrations}
          onclick={handleApplyAll}
        >
          Apply all migrations
        </button>
      </div>
    </div>
    <div class="mt-6 overflow-hidden rounded-lg border border-black/10">
      <table class="min-w-full border-collapse text-left text-sm">
        <thead class="bg-slate-100 text-xs font-semibold uppercase tracking-wide text-slate-600">
          <tr>
            <th class="px-4 py-3">ID</th>
            <th class="px-4 py-3">Introduced</th>
            <th class="px-4 py-3">Status</th>
            <th class="px-4 py-3">Applied at</th>
            <th class="px-4 py-3 text-right">Action</th>
          </tr>
        </thead>
        <tbody class="bg-white">
          {#if migrations.length === 0}
            <tr class="border-t border-black/5">
              <td class="px-4 py-4 text-slate-500" colspan="5">
                No migration metadata available yet.
              </td>
            </tr>
          {:else}
            {#each migrations as migration}
              <tr class="border-t border-black/5">
                <td class="px-4 py-3 font-medium text-slate-900">{migration.id}</td>
                <td class="px-4 py-3 text-slate-600">
                  {migration.introducedVersion}
                </td>
                <td class="px-4 py-3">
                  <span
                    class={`rounded-full px-3 py-1 text-xs font-semibold ${
                      migration.status === "applied"
                        ? "bg-emerald-100 text-emerald-700"
                        : "bg-amber-100 text-amber-700"
                    }`}
                  >
                    {migration.status === "applied" ? "Applied" : "Pending"}
                  </span>
                </td>
                <td class="px-4 py-3 text-slate-600">
                  {migration.appliedAt ?? "—"}
                </td>
              <td class="px-4 py-3 text-right text-slate-600">
                {#if migration.status === "pending"}
                  <button
                    type="button"
                    class="rounded-md border border-black/10 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:cursor-pointer"
                    onclick={() => handleApplyMigration(migration.id)}
                  >
                    Apply
                  </button>
                {/if}
              </td>
            </tr>
          {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </article>
</section>
