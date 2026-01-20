<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import Button from "$lib/components/Button.svelte";
  import {
    getAbsurdProvider,
    isTauriRuntime,
    type MigrationEntry,
    type SettingsInfo,
    type WorkerLogLine,
    type WorkerStatus,
  } from "$lib/providers/absurdData";
  import { invoke } from "@tauri-apps/api/core";
  import { revealItemInDir } from "@tauri-apps/plugin-opener";

  const provider = getAbsurdProvider();
  const defaults: SettingsInfo = {
    absurdVersion: "--",
    sqliteVersion: "--",
    dbPath: "--",
    dbSizeBytes: null,
    migration: {
      status: "missing",
      appliedCount: 0,
      latestVersion: null,
      latestAppliedAt: null,
    },
  };

  let settings = $state<SettingsInfo | null>(null);
  let migrations = $state<MigrationEntry[]>([]);
  let devApiStatus = $state<DevApiStatus | null>(null);
  let workerStatus = $state<WorkerStatus | null>(null);
  let workerPathDraft = $state("");
  let workerPathTouched = $state(false);
  let workerError = $state<string | null>(null);
  let migrationsError = $state<string | null>(null);
  let workerAction = $state<"idle" | "saving" | "starting" | "stopping">("idle");
  let workerLogs = $state<WorkerLogLine[]>([]);
  let workerLogsRef = $state<HTMLDivElement | null>(null);
  let workerLogsOpen = $state(false);
  let workerLogsError = $state<string | null>(null);
  let workerLogsTimer: ReturnType<typeof setInterval> | null = null;
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
  const canCopyPath = $derived(
    data.dbPath !== "--" && data.dbPath.trim().length > 0,
  );
  const canOpenFolder = $derived(canCopyPath && isTauriRuntime());
  let copyStatus = $state<"idle" | "copied" | "error">("idle");
  const showDevApi = $derived(isTauriRuntime());
  const normalizedWorkerPath = $derived(workerPathDraft.trim());
  const workerReady = $derived(workerStatus !== null);
  const dbSizeLabel = $derived.by(() => formatBytes(data.dbSizeBytes));
  const workerPathDirty = $derived(
    workerStatus ? (workerStatus.configuredPath ?? "") !== normalizedWorkerPath : false,
  );
  const workerPathConfigured = $derived(normalizedWorkerPath.length > 0);
  const migrationErrorMessage = (error: unknown) => {
    const message =
      error instanceof Error
        ? error.message
        : typeof error === "string"
          ? error
          : "Failed to apply migration.";
    if (message.includes("target version is newer than available migrations")) {
      return "Migration files are newer than the loaded SQLite extension. Rebuild the extension and restart the app.";
    }
    return message;
  };
  const workerStatusLabel = $derived.by(() => {
    if (!workerStatus) {
      return "Loading...";
    }
    if (workerStatus.crashing) {
      if (workerStatus.running && workerStatus.pid) {
        return `Crashing (PID ${workerStatus.pid})`;
      }
      return "Crashing";
    }
    if (workerStatus.running && workerStatus.pid) {
      return `Running (PID ${workerStatus.pid})`;
    }
    if (!workerStatus.configuredPath) {
      return "Not configured";
    }
    return "Stopped";
  });
  const workerStatusClasses = $derived.by(() => {
    if (!workerStatus) {
      return "bg-slate-100 text-slate-600";
    }
    if (workerStatus.crashing) {
      return "bg-rose-100 text-rose-700";
    }
    if (workerStatus.running) {
      return "bg-emerald-100 text-emerald-700";
    }
    return "bg-slate-100 text-slate-600";
  });
  const workerIndicatorClasses = $derived.by(() => {
    if (!workerStatus) {
      return "bg-slate-300";
    }
    if (workerStatus.crashing) {
      return "bg-rose-500";
    }
    if (workerStatus.running) {
      return "bg-emerald-500";
    }
    return "bg-slate-400";
  });

  const refreshData = async () => {
    settings = await provider.getSettingsInfo();
    migrations = await provider.getMigrations();
    workerStatus = await provider.getWorkerStatus();
    if (workerStatus && !workerPathTouched) {
      workerPathDraft = workerStatus.configuredPath ?? "";
    }
    if (showDevApi) {
      devApiStatus = await invoke<DevApiStatus>("get_dev_api_status");
    }
    if (workerLogsOpen) {
      await fetchWorkerLogs();
    }
  };

  const handleRefresh = () => {
    void refreshData();
  };

  const handleApplyAll = async () => {
    migrationsError = null;
    try {
      await provider.applyMigrationsAll();
      await refreshData();
    } catch (error) {
      migrationsError = migrationErrorMessage(error);
    }
  };

  const handleApplyMigration = async (migrationId: number) => {
    migrationsError = null;
    try {
      await provider.applyMigration(migrationId);
      await refreshData();
    } catch (error) {
      migrationsError = migrationErrorMessage(error);
    }
  };

  const handleCopyPath = async () => {
    if (!canCopyPath) {
      return;
    }

    try {
      await navigator.clipboard.writeText(data.dbPath);
      copyStatus = "copied";
    } catch {
      copyStatus = "error";
    }

    setTimeout(() => {
      copyStatus = "idle";
    }, 2000);
  };

  const resolveFolderPath = (path: string) => {
    if (!path || path === "--") return "";
    const normalized = path.replace(/\\/g, "/");
    const lastSlash = normalized.lastIndexOf("/");
    if (lastSlash <= 0) return path;
    const dir = normalized.slice(0, lastSlash);
    return path.includes("\\") ? dir.replaceAll("/", "\\") : dir;
  };

  const handleOpenFolder = async () => {
    if (!canOpenFolder) {
      return;
    }

    try {
      await revealItemInDir(data.dbPath);
    } catch (error) {
      console.error("Failed to open database folder", error);
    }
  };

  const syncWorkerStatus = (status: WorkerStatus) => {
    workerStatus = status;
    if (!workerPathTouched) {
      workerPathDraft = status.configuredPath ?? "";
    }
  };

  const handleSaveWorkerPath = async () => {
    workerError = null;
    workerAction = "saving";
    try {
      const status = await provider.setWorkerBinaryPath(normalizedWorkerPath);
      workerPathTouched = false;
      syncWorkerStatus(status);
    } catch (error) {
      workerError =
        error instanceof Error
          ? error.message
          : typeof error === "string"
            ? error
            : "Failed to save worker path.";
    } finally {
      workerAction = "idle";
    }
  };

  const handleStartWorker = async () => {
    workerError = null;
    workerAction = "starting";
    try {
      if (workerPathDirty) {
        const updated = await provider.setWorkerBinaryPath(normalizedWorkerPath);
        workerPathTouched = false;
        syncWorkerStatus(updated);
      }
      const status = await provider.startWorker();
      syncWorkerStatus(status);
    } catch (error) {
      workerError =
        error instanceof Error
          ? error.message
          : typeof error === "string"
            ? error
            : "Failed to start worker.";
    } finally {
      workerAction = "idle";
    }
  };

  const handleStopWorker = async () => {
    workerError = null;
    workerAction = "stopping";
    try {
      const status = await provider.stopWorker();
      syncWorkerStatus(status);
    } catch (error) {
      workerError =
        error instanceof Error
          ? error.message
          : typeof error === "string"
            ? error
            : "Failed to stop worker.";
    } finally {
      workerAction = "idle";
    }
  };

  const handleToggleWorker = async () => {
    if (workerStatus?.running) {
      await handleStopWorker();
    } else {
      await handleStartWorker();
    }
  };

  const fetchWorkerLogs = async () => {
    if (!workerLogsOpen) {
      return;
    }
    try {
      const data = await provider.getWorkerLogs();
      workerLogs = data.lines;
      requestAnimationFrame(() => {
        if (!workerLogsRef) {
          return;
        }
        workerLogsRef.scrollTop = workerLogsRef.scrollHeight;
      });
      workerLogsError = null;
    } catch (error) {
      workerLogsError =
        error instanceof Error
          ? error.message
          : typeof error === "string"
            ? error
            : "Failed to load worker logs.";
    }
  };

  const startWorkerLogsPolling = () => {
    if (workerLogsTimer) {
      return;
    }
    workerLogsTimer = setInterval(() => {
      void fetchWorkerLogs();
    }, 1000);
    void fetchWorkerLogs();
  };

  const stopWorkerLogsPolling = () => {
    if (workerLogsTimer) {
      clearInterval(workerLogsTimer);
      workerLogsTimer = null;
    }
  };

  const handleToggleWorkerLogs = () => {
    workerLogsOpen = !workerLogsOpen;
    if (workerLogsOpen) {
      startWorkerLogsPolling();
    } else {
      stopWorkerLogsPolling();
    }
  };

  onMount(() => {
    void refreshData();
  });

  onDestroy(() => {
    stopWorkerLogsPolling();
  });

  type DevApiStatus = {
    enabled: boolean;
    running: boolean;
    port: number | null;
    desiredPort: number;
  };

  const handleToggleDevApi = async () => {
    if (!devApiStatus) {
      return;
    }
    devApiStatus = await invoke<DevApiStatus>("set_dev_api_enabled", {
      enabled: !devApiStatus.enabled,
    });
  };

  function formatBytes(bytes: number | null) {
    if (bytes === null || Number.isNaN(bytes)) {
      return "—";
    }
    if (bytes < 1024) {
      return `${bytes} B`;
    }
    const units = ["KB", "MB", "GB", "TB"];
    let value = bytes / 1024;
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex += 1;
    }
    return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
  }
</script>

<section class="flex flex-wrap items-start justify-between gap-4">
  <div>
    <h1 class="text-3xl font-semibold text-slate-900">Settings</h1>
    <p class="mt-1 text-sm text-slate-600">
      Version details, storage location, and migration health.
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
    <p class="mt-1 text-sm text-slate-500">SQLite database information.</p>
    <dl class="mt-4 space-y-3 text-sm">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <dt class="text-slate-500">Disk size</dt>
        <dd class="font-medium text-slate-700">{dbSizeLabel}</dd>
      </div>
      <div class="flex flex-wrap items-center justify-between gap-3">
        <dt class="text-slate-500">Actions</dt>
        <dd class="flex flex-wrap items-center gap-2">
          <Button
            type="button"
            class="rounded-md border border-black/10 bg-white px-3 py-1 text-xs font-medium text-slate-700 disabled:cursor-not-allowed disabled:opacity-60"
            disabled={!canCopyPath}
            onclick={handleCopyPath}
          >
            {copyStatus === "copied"
              ? "Copied"
              : copyStatus === "error"
                ? "Copy failed"
                : "Copy full path"}
          </Button>
          <Button
            type="button"
            class="rounded-md border border-black/10 bg-white px-3 py-1 text-xs font-medium text-slate-700 disabled:cursor-not-allowed disabled:opacity-60"
            disabled={!canOpenFolder}
            onclick={handleOpenFolder}
          >
            Open folder
          </Button>
        </dd>
      </div>
    </dl>
  </article>

  <article class="rounded-lg border border-black/10 bg-white p-6 col-span-2">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div>
        <h2 class="text-2xl font-semibold text-slate-900">Worker</h2>
        <p class="mt-1 text-sm text-slate-500">
          Run a local worker process for this database.
        </p>
      </div>
      <div class="flex flex-wrap items-center gap-3">
        <span class={`h-2.5 w-2.5 rounded-full ${workerIndicatorClasses}`}></span>
        <span class={`rounded-full px-3 py-1 text-xs font-semibold ${workerStatusClasses}`}>
          {workerStatusLabel}
        </span>
        <Button
          type="button"
          class={`rounded-md px-3 py-1.5 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-60 ${
            workerStatus?.running
              ? "border border-rose-200 bg-rose-50 text-rose-700"
              : "border border-emerald-200 bg-emerald-50 text-emerald-700"
          }`}
          onclick={handleToggleWorker}
          disabled={!workerReady || !workerPathConfigured || workerAction !== "idle"}
        >
          {workerStatus?.running ? "Stop" : "Start"}
        </Button>
      </div>
    </div>
    <div class="mt-4 grid gap-4 lg:grid-cols-[1fr_auto]">
      <label class="flex flex-col gap-2 text-sm font-medium text-slate-600">
        <span class="flex flex-wrap items-baseline gap-4">
          <span>Command</span>
          <span class="text-xs font-normal text-slate-400">
            only configure worker commands that you trust to run on this machine.
          </span>
        </span>
        <input
          type="text"
          autocomplete="off"
          autocorrect="off"
          autocapitalize="off"
          spellcheck="false"
          class="w-full rounded-md border border-black/10 bg-white px-3 py-2 text-sm text-slate-700"
          placeholder="npx absurd-worker"
          bind:value={workerPathDraft}
          oninput={() => {
            workerPathTouched = true;
          }}
        />
      </label>
      <div class="flex flex-wrap items-end gap-2">
        <Button
          type="button"
          class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700 disabled:cursor-not-allowed disabled:opacity-60"
          onclick={handleSaveWorkerPath}
          disabled={!workerReady || !workerPathDirty || workerAction !== "idle"}
        >
          Save
        </Button>
      </div>
    </div>
    <div class="mt-4 flex flex-wrap items-center gap-3">
      <Button
        type="button"
        class="rounded-md border border-black/10 bg-white px-3 py-1.5 text-xs font-semibold text-slate-600"
        onclick={handleToggleWorkerLogs}
        disabled={!workerReady}
      >
        {workerLogsOpen ? "Hide logs" : "Show logs"}
      </Button>
    </div>
    {#if workerLogsOpen}
      <div class="mt-3 rounded-md border border-black/10 bg-slate-50">
        <div
          class="h-56 overflow-auto px-3 py-2 font-mono text-xs text-slate-700"
          bind:this={workerLogsRef}
        >
          {#if workerLogs.length === 0}
            <p class="text-slate-400">No logs yet.</p>
          {:else}
            {#each workerLogs as logLine}
              <div class="flex gap-2">
                <span class="text-slate-400">{logLine.timestamp}</span>
                <span class="uppercase text-slate-500">{logLine.stream}</span>
                <span class="whitespace-pre-wrap">{logLine.line}</span>
              </div>
            {/each}
          {/if}
        </div>
      </div>
      {#if workerLogsError}
        <p class="mt-2 text-xs text-rose-600">{workerLogsError}</p>
      {/if}
    {/if}
    {#if workerError}
      <p class="mt-3 text-sm text-rose-600">{workerError}</p>
    {/if}
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
        <Button
          type="button"
          class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700 disabled:cursor-not-allowed disabled:opacity-60"
          disabled={!hasPendingMigrations}
          onclick={handleApplyAll}
        >
          Apply all migrations
        </Button>
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
                  <Button
                    type="button"
                    class="rounded-md border border-black/10 bg-white px-3 py-1 text-xs font-medium text-slate-700"
                    onclick={() => handleApplyMigration(migration.id)}
                  >
                    Apply
                  </Button>
                {/if}
              </td>
            </tr>
          {/each}
          {/if}
        </tbody>
      </table>
    </div>
    {#if migrationsError}
      <p class="mt-3 text-sm text-rose-600">{migrationsError}</p>
    {/if}
  </article>

  {#if showDevApi}
    <article class="rounded-lg border border-black/10 bg-white p-6 col-span-2">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 class="text-2xl font-semibold text-slate-900">Developer</h2>
        </div>
      </div>
      <div class="mt-4 flex flex-wrap items-center justify-between gap-4 rounded-lg border border-black/10 px-4 py-3 text-sm">
        <div class="flex items-center gap-3">
          <span
            class={`h-2.5 w-2.5 rounded-full ${
              devApiStatus?.running ? "bg-emerald-500" : "bg-slate-400"
            }`}
          ></span>
          <span class="text-slate-700">
            Dev API server:
            {#if devApiStatus?.running && devApiStatus?.port}
              <span class="ml-2 rounded-md bg-slate-900 px-2 py-1 font-mono text-xs text-white">
                {`http://localhost:${devApiStatus.port}`}
              </span>
            {:else}
              <span class="ml-2 text-slate-500">Disabled</span>
            {/if}
          </span>
        </div>
        <Button
          type="button"
          class="rounded-md border border-black/10 bg-white px-4 py-2 text-sm font-medium text-slate-700"
          onclick={handleToggleDevApi}
          disabled={!devApiStatus}
        >
          {devApiStatus?.enabled ? "Disable" : "Enable"}
        </Button>
      </div>
    </article>
  {/if}
</section>
