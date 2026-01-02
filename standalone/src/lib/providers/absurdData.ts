import { createTRPCProxyClient, httpLink } from "@trpc/client";

export type OverviewMetrics = {
  activeQueues: number;
  messagesProcessed: number;
  messagesInQueue: number;
  visibleNow: number;
};

export type QueueMetric = {
  name: string;
  inQueue: number;
  visible: number;
  newestAge: string;
  oldestAge: string;
  totalSeen: number;
  scrapedAt: string;
};

export type TaskRun = {
  id: string;
  name: string;
  queue: string;
  status: "running" | "failed" | "completed" | "sleeping" | "pending" | "cancelled";
  attempt: string;
  attemptNumber: number;
  runId: string;
  age: string;
  startedAt: string;
  updatedAt: string;
  createdAgo: string;
  updatedAgo: string;
  paramsSummary: string;
  paramsJson: string;
  finalStateJson?: string;
  worker: string;
};

export type TaskInfo = {
  id: string;
  name: string;
  queue: string;
};

export type TaskRunFilters = {
  queueName?: string;
  status?: TaskRun["status"];
  taskName?: string;
  search?: string;
  limit?: number;
};

export type TaskRunPage = {
  runs: TaskRun[];
  totalCount: number;
};

export type QueueSummary = {
  name: string;
  createdAt: string;
  age: string;
  stats: {
    label: string;
    value: number;
  }[];
};

export type EventFilterDefaults = {
  eventNamePlaceholder: string;
  queueLabel: string;
  queueOptions: string[];
};

export type EventEntry = {
  id: string;
  name: string;
  queue: string;
  createdAt: string;
  payloadPreview: string;
};

export type MigrationStatus = {
  status: "applied" | "missing";
  appliedCount: number;
  latestVersion: string | null;
  latestAppliedAt: string | null;
};

export type SettingsInfo = {
  absurdVersion: string;
  sqliteVersion: string;
  dbPath: string;
  dbSizeBytes: number | null;
  migration: MigrationStatus;
};

export type WorkerStatus = {
  configuredPath: string | null;
  running: boolean;
  pid: number | null;
  crashing: boolean;
};

export type WorkerLogLine = {
  timestamp: string;
  stream: "stdout" | "stderr";
  line: string;
};

export type WorkerLogs = {
  lines: WorkerLogLine[];
};

export type MigrationEntry = {
  id: number;
  introducedVersion: string;
  appliedAt: string | null;
  status: "applied" | "pending";
};

export type AbsurdDataProvider = {
  getOverviewMetrics: () => Promise<OverviewMetrics>;
  getQueueMetrics: () => Promise<QueueMetric[]>;
  getTaskRuns: () => Promise<TaskRun[]>;
  getTaskRunsForQueue: (queueName: string) => Promise<TaskRun[]>;
  getTaskRunsPage: (filters: TaskRunFilters) => Promise<TaskRunPage>;
  getTaskHistory: (taskId: string) => Promise<TaskRun[]>;
  getTaskInfo: (taskId: string) => Promise<TaskInfo | null>;
  getQueueNames: () => Promise<string[]>;
  getTaskNameOptions: (queueName?: string) => Promise<string[]>;
  getQueueSummaries: () => Promise<QueueSummary[]>;
  createQueue: (queueName: string) => Promise<void>;
  getEventFilterDefaults: (queueName?: string) => Promise<EventFilterDefaults>;
  getEvents: () => Promise<EventEntry[]>;
  getFilteredEvents: (filters: { queueName?: string; eventName?: string }) => Promise<EventEntry[]>;
  getSettingsInfo: () => Promise<SettingsInfo>;
  getWorkerStatus: () => Promise<WorkerStatus>;
  getWorkerLogs: () => Promise<WorkerLogs>;
  setWorkerBinaryPath: (path: string) => Promise<WorkerStatus>;
  startWorker: () => Promise<WorkerStatus>;
  stopWorker: () => Promise<WorkerStatus>;
  getMigrations: () => Promise<MigrationEntry[]>;
  applyMigrationsAll: () => Promise<number>;
  applyMigration: (migrationId: number) => Promise<number>;
};

export const isTauriRuntime = () =>
  typeof window !== "undefined" && Boolean((window as { __TAURI__?: unknown }).__TAURI__);

const tauriInvoke = async <T>(command: string, args?: Record<string, unknown>): Promise<T> => {
  const { invoke } = await import("@tauri-apps/api/core");
  return invoke<T>(command, args);
};

export const tauriAbsurdProvider: AbsurdDataProvider = {
  getOverviewMetrics: () => tauriInvoke("get_overview_metrics"),
  getQueueMetrics: () => tauriInvoke("get_queue_metrics"),
  getTaskRuns: () => tauriInvoke("get_task_runs"),
  getTaskRunsForQueue: (queueName) =>
    tauriInvoke("get_task_runs_for_queue", { queue_name: queueName }),
  getTaskRunsPage: (filters) => tauriInvoke("get_task_runs_page", { filters }),
  getTaskHistory: (taskId) => tauriInvoke("get_task_history", { taskId }),
  getTaskInfo: (taskId) => tauriInvoke("get_task_info", { taskId }),
  getQueueNames: () => tauriInvoke("get_queue_names"),
  getTaskNameOptions: (queueName) =>
    tauriInvoke("get_task_name_options", { queue_name: queueName ?? null }),
  getQueueSummaries: () => tauriInvoke("get_queue_summaries"),
  createQueue: (queueName) => tauriInvoke("create_queue", { queueName }),
  getEventFilterDefaults: (queueName) =>
    tauriInvoke("get_event_filter_defaults", queueName ? { queue_name: queueName } : undefined),
  getEvents: () => tauriInvoke("get_events"),
  getFilteredEvents: (filters) => tauriInvoke("get_filtered_events", { filters }),
  getSettingsInfo: () => tauriInvoke("get_settings_info"),
  getWorkerStatus: () => tauriInvoke("get_worker_status"),
  getWorkerLogs: () => tauriInvoke("get_worker_logs"),
  setWorkerBinaryPath: (path) => tauriInvoke("set_worker_binary_path", { path }),
  startWorker: () => tauriInvoke("start_worker"),
  stopWorker: () => tauriInvoke("stop_worker"),
  getMigrations: () => tauriInvoke("get_migrations"),
  applyMigrationsAll: () => tauriInvoke("apply_migrations_all"),
  applyMigration: (migrationId) =>
    tauriInvoke("apply_migration", { migration_id: migrationId }),
};

const mockMigrations: MigrationEntry[] = [
  {
    id: 1,
    introducedVersion: "0.1.0",
    appliedAt: "Dec 27, 2025, 2:48 PM",
    status: "applied",
  },
  {
    id: 2,
    introducedVersion: "0.2.0",
    appliedAt: null,
    status: "pending",
  },
];

let mockWorkerStatus: WorkerStatus = {
  configuredPath: "/usr/local/bin/absurd-worker",
  running: false,
  pid: null,
  crashing: false,
};

const mockWorkerLogs: WorkerLogLine[] = [
  { timestamp: "12:01:22", stream: "stdout", line: "worker started" },
  { timestamp: "12:01:23", stream: "stderr", line: "warning: sample log output" },
  { timestamp: "12:01:24", stream: "stdout", line: "ready for tasks" },
];

const DEV_API_PORT_BASE = 11223;
const DEV_API_PORT_ATTEMPTS = 10;
const DEV_API_REQUEST_TIMEOUT_MS = 400;
const DEV_API_RETRY_DELAY_MS = 2000;

let devApiBaseUrl: string | null | undefined = undefined;
let devApiLastFailureMs: number | null = null;
let devApiResolveInFlight: Promise<string | null> | null = null;
let trpcClient: ReturnType<typeof createTRPCProxyClient<any>> | null = null;
let trpcClientBaseUrl: string | null = null;

const resolveDevApiBaseUrl = async (): Promise<string | null> => {
  if (devApiBaseUrl === null && devApiLastFailureMs !== null) {
    if (Date.now() - devApiLastFailureMs < DEV_API_RETRY_DELAY_MS) {
      return null;
    }
    devApiBaseUrl = undefined;
  }

  if (devApiBaseUrl !== undefined) {
    return devApiBaseUrl;
  }

  if (devApiResolveInFlight) {
    return devApiResolveInFlight;
  }

  devApiResolveInFlight = (async () => {
    for (let attempt = 0; attempt < DEV_API_PORT_ATTEMPTS; attempt += 1) {
      const port = DEV_API_PORT_BASE + attempt;
      const baseUrl = `http://localhost:${port}`;
      const ok = await probeDevApi(baseUrl);
      if (ok) {
        devApiBaseUrl = baseUrl;
        devApiLastFailureMs = null;
        return baseUrl;
      }
    }
    devApiBaseUrl = null;
    devApiLastFailureMs = Date.now();
    return null;
  })();

  const resolved = await devApiResolveInFlight;
  devApiResolveInFlight = null;
  return resolved;
};

const probeDevApi = async (baseUrl: string): Promise<boolean> => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DEV_API_REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(`${baseUrl}/absurd-data/health`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id: 1, json: null }),
      signal: controller.signal,
    });
    if (!response.ok) {
      return false;
    }
    const payload = (await response.json()) as {
      result?: { data?: { ok?: boolean } };
    };
    return Boolean(payload.result?.data?.ok);
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
  }
};

const getTrpcClient = async () => {
  const baseUrl = await resolveDevApiBaseUrl();
  if (!baseUrl) {
    return null;
  }

  if (!trpcClient || trpcClientBaseUrl !== baseUrl) {
    trpcClientBaseUrl = baseUrl;
    trpcClient = createTRPCProxyClient<any>({
      links: [
        httpLink({
          url: `${baseUrl}/absurd-data`,
        }),
      ],
    });
  }

  return trpcClient;
};

const trpcQuery = async <T>(procedure: string, input?: unknown): Promise<T> => {
  const client = await getTrpcClient();
  if (!client) {
    throw new Error("Dev API server not available");
  }
  const caller = (
    client as unknown as Record<string, { query: (value?: unknown) => Promise<T> }>
  )[procedure];
  if (!caller?.query) {
    throw new Error(`Unknown tRPC query ${procedure}`);
  }
  return caller.query(input);
};

const trpcMutation = async <T>(procedure: string, input?: unknown): Promise<T> => {
  const client = await getTrpcClient();
  if (!client) {
    throw new Error("Dev API server not available");
  }
  const caller = (
    client as unknown as Record<string, { mutate: (value?: unknown) => Promise<T> }>
  )[procedure];
  if (!caller?.mutate) {
    throw new Error(`Unknown tRPC mutation ${procedure}`);
  }
  return caller.mutate(input);
};

const trpcAbsurdProvider: AbsurdDataProvider = {
  getOverviewMetrics: () => trpcQuery("getOverviewMetrics"),
  getQueueMetrics: () => trpcQuery("getQueueMetrics"),
  getTaskRuns: () => trpcQuery("getTaskRuns"),
  getTaskRunsForQueue: (queueName) =>
    trpcQuery("getTaskRunsForQueue", { queueName }),
  getTaskRunsPage: (filters) => trpcQuery("getTaskRunsPage", filters),
  getTaskHistory: (taskId) => trpcQuery("getTaskHistory", { taskId }),
  getTaskInfo: (taskId) => trpcQuery("getTaskInfo", { taskId }),
  getQueueNames: () => trpcQuery("getQueueNames"),
  getTaskNameOptions: (queueName) =>
    trpcQuery("getTaskNameOptions", queueName ? { queueName } : null),
  getQueueSummaries: () => trpcQuery("getQueueSummaries"),
  createQueue: (queueName) => trpcMutation("createQueue", { queueName }),
  getEventFilterDefaults: (queueName) =>
    trpcQuery("getEventFilterDefaults", queueName ? { queueName } : null),
  getEvents: () => trpcQuery("getEvents"),
  getFilteredEvents: (filters) => trpcQuery("getFilteredEvents", filters),
  getSettingsInfo: () => trpcQuery("getSettingsInfo"),
  getWorkerStatus: () => trpcQuery("getWorkerStatus"),
  getWorkerLogs: () => trpcQuery("getWorkerLogs"),
  setWorkerBinaryPath: (path) => trpcMutation("setWorkerBinaryPath", { path }),
  startWorker: () => trpcMutation("startWorker"),
  stopWorker: () => trpcMutation("stopWorker"),
  getMigrations: () => trpcQuery("getMigrations"),
  applyMigrationsAll: () => trpcMutation("applyMigrationsAll"),
  applyMigration: (migrationId) =>
    trpcMutation("applyMigration", { migrationId }),
};

export const getAbsurdProvider = (): AbsurdDataProvider =>
  isTauriRuntime() ? tauriAbsurdProvider : trpcAbsurdProvider;

export const mockAbsurdProvider: AbsurdDataProvider = {
  getQueueNames: async () =>
    (await mockAbsurdProvider.getQueueSummaries()).map((queue) => queue.name),
  getTaskNameOptions: async (queueName?: string) => {
    const runs = await mockAbsurdProvider.getTaskRuns();
    const filtered = queueName && queueName !== "All queues"
      ? runs.filter((run) => run.queue === queueName)
      : runs;
    return [...new Set(filtered.map((run) => run.name))].sort();
  },
  createQueue: async () => {},
  getOverviewMetrics: async () => ({
    activeQueues: 1,
    messagesProcessed: 0,
    messagesInQueue: 0,
    visibleNow: 0,
  }),
  getQueueMetrics: async () => [
    {
      name: "test",
      inQueue: 0,
      visible: 0,
      newestAge: "—",
      oldestAge: "—",
      totalSeen: 0,
      scrapedAt: "Dec 27, 2025, 2:48:45 PM",
    },
  ],
  getTaskRuns: async () => [
    {
      id: "019b470c-a9e6-70c7-aba8-d57f79368ba2",
      name: "test",
      queue: "default",
      status: "running",
      attempt: "5 / ∞",
      attemptNumber: 5,
      runId: "019b4714-040d-76bd-aa15-e3d372e9f53f",
      age: "5d",
      startedAt: "Dec 22, 2025, 9:13 AM",
      updatedAt: "Dec 27, 2025, 3:03 PM",
      createdAgo: "5d ago",
      updatedAgo: "5d ago",
      paramsSummary: "{ \"tenant\": \"absurd\", \"retries\": 4 }",
      paramsJson: "{\n  \"tenant\": \"absurd\",\n  \"retries\": 4\n}",
      worker: "mordor.local:89695",
    },
    {
      id: "019b470c-a9e6-70c7-aba8-d57f79368ba2",
      name: "test",
      queue: "default",
      status: "failed",
      attempt: "4 / ∞",
      attemptNumber: 4,
      runId: "019b4712-2d47-77c6-b3a0-bd49d125c35b",
      age: "5d",
      startedAt: "Dec 22, 2025, 9:03 AM",
      updatedAt: "Dec 22, 2025, 9:05 AM",
      createdAgo: "5d ago",
      updatedAgo: "5d ago",
      paramsSummary: "{ \"tenant\": \"absurd\", \"retries\": 3 }",
      paramsJson: "{\n  \"tenant\": \"absurd\",\n  \"retries\": 3\n}",
      finalStateJson:
        "{\n  \"name\": \"SqsClaimTimeout\",\n  \"message\": \"Worker did not finish task within claim interval\",\n  \"worker\": \"mordor.local:18695\",\n  \"claimExpiredAt\": \"2025-12-22T09:05:01.006Z\"\n}",
      worker: "mordor.local:89695",
    },
    {
      id: "019b470c-a9e6-70c7-aba8-d57f79368ba2",
      name: "test",
      queue: "default",
      status: "failed",
      attempt: "3 / ∞",
      attemptNumber: 3,
      runId: "019b4710-568e-73a7-aaee-e2bed95c4936",
      age: "5d",
      startedAt: "Dec 22, 2025, 9:01 AM",
      updatedAt: "Dec 22, 2025, 9:03 AM",
      createdAgo: "5d ago",
      updatedAgo: "5d ago",
      paramsSummary: "{ \"tenant\": \"absurd\", \"retries\": 2 }",
      paramsJson: "{\n  \"tenant\": \"absurd\",\n  \"retries\": 2\n}",
      finalStateJson:
        "{\n  \"name\": \"SqsClaimTimeout\",\n  \"message\": \"Worker did not finish task within claim interval\",\n  \"worker\": \"mordor.local:18695\",\n  \"claimExpiredAt\": \"2025-12-22T09:03:01.006Z\"\n}",
      worker: "mordor.local:89695",
    },
    {
      id: "019b470c-a9e6-70c7-aba8-d57f79368ba2",
      name: "test",
      queue: "default",
      status: "failed",
      attempt: "2 / ∞",
      attemptNumber: 2,
      runId: "019b470e-8014-7aa3-83de-9c3cf715a103",
      age: "5d",
      startedAt: "Dec 22, 2025, 8:59 AM",
      updatedAt: "Dec 22, 2025, 9:01 AM",
      createdAgo: "5d ago",
      updatedAgo: "5d ago",
      paramsSummary: "{ \"tenant\": \"absurd\", \"retries\": 1 }",
      paramsJson: "{\n  \"tenant\": \"absurd\",\n  \"retries\": 1\n}",
      finalStateJson:
        "{\n  \"name\": \"SqsClaimTimeout\",\n  \"message\": \"Worker did not finish task within claim interval\",\n  \"worker\": \"mordor.local:18695\",\n  \"claimExpiredAt\": \"2025-12-22T09:01:01.006Z\"\n}",
      worker: "mordor.local:89695",
    },
    {
      id: "019b470c-a9e6-70c7-aba8-d57f79368ba2",
      name: "test",
      queue: "default",
      status: "failed",
      attempt: "1 / ∞",
      attemptNumber: 1,
      runId: "019b470c-a9e9-7cf1-aea5-73182e8773eb",
      age: "5d",
      startedAt: "Dec 22, 2025, 8:57 AM",
      updatedAt: "Dec 22, 2025, 8:59 AM",
      createdAgo: "5d ago",
      updatedAgo: "5d ago",
      paramsSummary: "{ \"tenant\": \"absurd\", \"retries\": 0 }",
      paramsJson: "{\n  \"tenant\": \"absurd\",\n  \"retries\": 0\n}",
      finalStateJson:
        "{\n  \"name\": \"SqsClaimTimeout\",\n  \"message\": \"Worker did not finish task within claim interval\",\n  \"worker\": \"mordor.local:18695\",\n  \"claimExpiredAt\": \"2025-12-22T08:59:01.006Z\"\n}",
      worker: "mordor.local:89695",
    },
    {
      id: "019b470c-ad55-7763-b622-6505a1b74c9b",
      name: "test2",
      queue: "default",
      status: "completed",
      attempt: "1 / ∞",
      attemptNumber: 1,
      runId: "019b470c-ad58-72e6-935d-6855f1c0e568",
      age: "5d",
      startedAt: "Dec 22, 2025, 8:20 AM",
      updatedAt: "Dec 22, 2025, 8:21 AM",
      createdAgo: "5d ago",
      updatedAgo: "5d ago",
      paramsSummary: "{ \"source\": \"nightly\" }",
      paramsJson: "{\n  \"source\": \"nightly\"\n}",
      worker: "mordor.local:77102",
    },
    {
      id: "019b470c-246b-73f0-876d-f76a9818ac5a",
      name: "test",
      queue: "default",
      status: "sleeping",
      attempt: "1 / ∞",
      attemptNumber: 1,
      runId: "019b470c-246e-7f6e-a245-f6e38bad94f1",
      age: "5d",
      startedAt: "Dec 22, 2025, 7:10 AM",
      updatedAt: "Dec 22, 2025, 7:12 AM",
      createdAgo: "5d ago",
      updatedAgo: "5d ago",
      paramsSummary: "{ \"stage\": \"backoff\" }",
      paramsJson: "{\n  \"stage\": \"backoff\"\n}",
      worker: "mordor.local:99210",
    },
  ],
  getTaskRunsForQueue: async (queueName: string) =>
    queueName === "All queues"
      ? mockAbsurdProvider.getTaskRuns()
      : (await mockAbsurdProvider.getTaskRuns()).filter((run) => run.queue === queueName),
  getTaskRunsPage: async (filters) => {
    const runs = await mockAbsurdProvider.getTaskRuns();
    const normalizedQueue = (filters.queueName ?? "All queues").toLowerCase();
    const status = (filters.status ?? "").toLowerCase();
    const taskName = (filters.taskName ?? "").trim();
    const search = (filters.search ?? "").trim().toLowerCase();
    const limit = filters.limit ?? 500;
    const filtered = runs.filter((run) => {
      if (normalizedQueue !== "all queues" && run.queue.toLowerCase() !== normalizedQueue) {
        return false;
      }
      if (status && run.status.toLowerCase() !== status) {
        return false;
      }
      if (taskName && run.name !== taskName) {
        return false;
      }
      if (search) {
        const haystack = [
          run.id,
          run.runId,
          run.name,
          run.queue,
          run.status,
          run.paramsSummary,
          run.paramsJson,
          run.finalStateJson ?? "",
          run.worker,
        ]
          .join(" ")
          .toLowerCase();
        if (!haystack.includes(search)) {
          return false;
        }
      }
      return true;
    });
    const limited = limit > 0 ? filtered.slice(0, limit) : filtered;
    return { runs: limited, totalCount: filtered.length };
  },
  getTaskHistory: async (taskId: string) =>
    (await mockAbsurdProvider.getTaskRuns()).filter((run) => run.id === taskId),
  getTaskInfo: async (taskId: string) => {
    const run = (await mockAbsurdProvider.getTaskRuns()).find((item) => item.id === taskId);
    if (!run) {
      return null;
    }
    return { id: run.id, name: run.name, queue: run.queue };
  },
  getQueueSummaries: async () => [
    {
      name: "default",
      createdAt: "Created Dec 19, 2025, 5:25 PM",
      age: "8d ago",
      stats: [
        { label: "Pending", value: 0 },
        { label: "Running", value: 1 },
        { label: "Sleeping", value: 1 },
        { label: "Completed", value: 3 },
        { label: "Failed", value: 0 },
        { label: "Cancelled", value: 0 },
      ],
    },
    {
      name: "test",
      createdAt: "Created Dec 19, 2025, 5:20 PM",
      age: "8d ago",
      stats: [
        { label: "Pending", value: 0 },
        { label: "Running", value: 0 },
        { label: "Sleeping", value: 0 },
        { label: "Completed", value: 0 },
        { label: "Failed", value: 0 },
        { label: "Cancelled", value: 0 },
      ],
    },
  ],
  getEventFilterDefaults: async (queueName?: string) => ({
    eventNamePlaceholder: "payment.completed",
    queueLabel: queueName ?? "All queues",
    queueOptions: ["All queues", ...(await mockAbsurdProvider.getQueueNames())],
  }),
  getEvents: async () => [
    {
      id: "evt_019b4714",
      name: "task.started",
      queue: "default",
      createdAt: "Dec 27, 2025, 2:48:45 PM",
      payloadPreview: "{ \"taskId\": \"019b470c-a9e6\", \"runId\": \"019b4714\" }",
    },
    {
      id: "evt_019b4712",
      name: "task.failed",
      queue: "default",
      createdAt: "Dec 27, 2025, 2:46:12 PM",
      payloadPreview: "{ \"taskId\": \"019b470c-a9e6\", \"attempt\": 4 }",
    },
    {
      id: "evt_019b470e",
      name: "task.failed",
      queue: "test",
      createdAt: "Dec 27, 2025, 2:42:05 PM",
      payloadPreview: "{ \"taskId\": \"019b470c-246b\", \"attempt\": 1 }",
    },
  ],
  getFilteredEvents: async ({ queueName, eventName }) => {
    const normalizedQueue = (queueName ?? "All queues").toLowerCase();
    const normalizedEventName = (eventName ?? "").trim().toLowerCase();
    const events = await mockAbsurdProvider.getEvents();
    return events.filter((event) => {
      const queueMatch =
        normalizedQueue === "all queues" || event.queue.toLowerCase() === normalizedQueue;
      const nameMatch =
        normalizedEventName.length === 0 || event.name.toLowerCase().includes(normalizedEventName);
      return queueMatch && nameMatch;
    });
  },
  getSettingsInfo: async () => ({
    absurdVersion: "absurd-sqlite/0.0.0",
    sqliteVersion: "3.45.0",
    dbPath: "/Users/demo/Library/Application Support/absurd-sqlite.db",
    dbSizeBytes: 7340032,
    migration: {
      status: "applied",
      appliedCount: 1,
      latestVersion: "0.1.0",
      latestAppliedAt: "Dec 27, 2025, 2:48 PM",
    },
  }),
  getWorkerStatus: async () => ({ ...mockWorkerStatus }),
  getWorkerLogs: async () => ({ lines: [...mockWorkerLogs] }),
  setWorkerBinaryPath: async (path: string) => {
    mockWorkerStatus = {
      ...mockWorkerStatus,
      configuredPath: path.trim().length > 0 ? path.trim() : null,
    };
    return { ...mockWorkerStatus };
  },
  startWorker: async () => {
    if (mockWorkerStatus.configuredPath) {
      mockWorkerStatus = {
        ...mockWorkerStatus,
        running: true,
        pid: Math.floor(Math.random() * 40000) + 1000,
        crashing: false,
      };
    }
    return { ...mockWorkerStatus };
  },
  stopWorker: async () => {
    mockWorkerStatus = {
      ...mockWorkerStatus,
      running: false,
      pid: null,
    };
    return { ...mockWorkerStatus };
  },
  getMigrations: async () => mockMigrations.map((entry) => ({ ...entry })),
  applyMigrationsAll: async () => {
    let applied = 0;
    for (const entry of mockMigrations) {
      if (entry.status === "pending") {
        entry.status = "applied";
        entry.appliedAt = "Dec 27, 2025, 3:15 PM";
        applied += 1;
      }
    }
    return applied;
  },
  applyMigration: async (migrationId: number) => {
    const entry = mockMigrations.find((row) => row.id === migrationId);
    if (!entry || entry.status === "applied") {
      return 0;
    }
    entry.status = "applied";
    entry.appliedAt = "Dec 27, 2025, 3:15 PM";
    return 1;
  },
};
