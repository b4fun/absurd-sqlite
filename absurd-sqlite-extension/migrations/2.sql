-- introduced_version: 0.1.0
-- Speed up task cleanup cascades by indexing waits on task ownership.

create index if not exists absurd_waits_task_idx
  on absurd_waits (queue_name, task_id);
