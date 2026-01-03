# Queue Cleanup UI (Standalone)

## Goal
Provide a queue-scoped cleanup action in the standalone app to remove old terminal tasks or events.

## Scope
- Add a "Clean Up" button to each queue card on `/queues`.
- Show a modal with cleanup options for tasks or events.
- Introduce a backend cleanup command and provider method to perform cleanup and return a deleted count.

## User Experience
- Queue card actions: `Tasks →`, `Events →`, `Clean Up` (same visual style as existing buttons).
- Clicking `Clean Up` opens a modal dialog scoped to that queue.

### Modal UI
- Title: `Clean up {queue.name}`
- Target selector: `Tasks` | `Events` (default: `Tasks`).

#### Tasks target
- Age selection (button group):
  - > 7d
  - > 30d
  - All
- Helper text: "Pending, running, and sleeping tasks will be preserved."

#### Events target
- Age selection (button group):
  - > 7d
  - > 30d
  - All

### Actions
- Primary: `Clean up` (disabled while running)
- Secondary: `Cancel`
- On success: close modal and refresh queue summaries.

## Option Mapping
### Age filter (ttlSeconds)
- > 7d => `7 * 24 * 60 * 60`
- > 30d => `30 * 24 * 60 * 60`
- All => `0` (delete everything older than now)

## Backend/API
- Provider method in `standalone/src/lib/providers/absurdData.ts`:
  - `cleanupQueue(options): Promise<{ deletedCount: number }>`
  - Options:
    - `queueName: string`
    - `target: "tasks" | "events"`
    - `ttlSeconds: number`

### Tauri + Dev API
- Command: `cleanup_queue` in `standalone/src-tauri/src/db_commands.rs`
- Dev API procedure: `cleanupQueue`

### Execution approach
- Use existing extension functions for cleanup:
  - `absurd_cleanup_tasks(queue, ttlSeconds, limit)`
  - `absurd_cleanup_events(queue, ttlSeconds, limit)`
- Batch deletion with a per-call limit (e.g. 500 or 1000). If user selects `All` age, loop until zero deleted.

## Error Handling
- Show inline error text in the modal if cleanup fails.
- Disable controls during cleanup to prevent double submits.
