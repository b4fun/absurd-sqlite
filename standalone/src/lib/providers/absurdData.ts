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

export type AbsurdDataProvider = {
  getOverviewMetrics: () => Promise<OverviewMetrics>;
  getQueueMetrics: () => Promise<QueueMetric[]>;
  getTaskRuns: () => Promise<TaskRun[]>;
  getTaskRunsForQueue: (queueName: string) => Promise<TaskRun[]>;
  getTaskHistory: (taskId: string) => Promise<TaskRun[]>;
  getQueueNames: () => Promise<string[]>;
  getQueueSummaries: () => Promise<QueueSummary[]>;
  getEventFilterDefaults: (queueName?: string) => Promise<EventFilterDefaults>;
  getEvents: () => Promise<EventEntry[]>;
  getFilteredEvents: (filters: { queueName?: string; eventName?: string }) => Promise<EventEntry[]>;
};

const isTauriRuntime = () =>
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
  getTaskHistory: (taskId) => tauriInvoke("get_task_history", { task_id: taskId }),
  getQueueNames: () => tauriInvoke("get_queue_names"),
  getQueueSummaries: () => tauriInvoke("get_queue_summaries"),
  getEventFilterDefaults: (queueName) =>
    tauriInvoke("get_event_filter_defaults", queueName ? { queue_name: queueName } : undefined),
  getEvents: () => tauriInvoke("get_events"),
  getFilteredEvents: (filters) => tauriInvoke("get_filtered_events", { filters }),
};

export const getAbsurdProvider = (): AbsurdDataProvider =>
  isTauriRuntime() ? tauriAbsurdProvider : mockAbsurdProvider;

export const mockAbsurdProvider: AbsurdDataProvider = {
  getQueueNames: async () =>
    (await mockAbsurdProvider.getQueueSummaries()).map((queue) => queue.name),
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
  getTaskHistory: async (taskId: string) =>
    (await mockAbsurdProvider.getTaskRuns()).filter((run) => run.id === taskId),
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
};
