# Absurd SQLite Bun Worker

Utilities for running Absurd-SQLite workers in Bun.

## Installation

```bash
bun add @absurd-sqlite/bun-worker
```

## Usage

### Basic Setup

Set the database path and extension path via environment variables:

- `ABSURD_DATABASE_PATH` (required): path to the SQLite database file.
- `ABSURD_DATABASE_EXTENSION_PATH` (optional): path to the Absurd-SQLite extension (`libabsurd.*`).
- `ABSURD_DOWNLOAD_EXTENSION` (optional): set to `"true"` to enable automatic extension download.

```typescript
import run from "@absurd-sqlite/bun-worker";

export default run(async (absurd) => {
  absurd.registerTask("hello", async (ctx, params) => {
    return { ok: true };
  });
});
```

### Automatic Extension Download

The SDK can automatically download the appropriate extension for your platform:

```typescript
import run from "@absurd-sqlite/bun-worker";

// Option 1: Enable via environment variable
// ABSURD_DOWNLOAD_EXTENSION=true bun run worker.ts

// Option 2: Specify download options in code
export default run(
  async (absurd) => {
    absurd.registerTask("hello", async (ctx, params) => {
      return { ok: true };
    });
  },
  {
    downloadOptions: { version: "latest" }
  }
);

// Option 3: Specify a specific version
export default run(
  async (absurd) => {
    absurd.registerTask("hello", async (ctx, params) => {
      return { ok: true };
    });
  },
  {
    downloadOptions: { version: "v0.1.0-alpha.3" }
  }
);
```

The extension is cached in `~/.cache/absurd-sqlite/extensions/` to avoid repeated downloads.

### Extension Resolution Order

The extension path is resolved in the following order:

1. `extensionPath` in `WorkerOptions`
2. `ABSURD_DATABASE_EXTENSION_PATH` environment variable
3. `ABSURD_SQLITE_EXTENSION_PATH` environment variable (fallback)
4. Automatic download from GitHub releases (if `downloadOptions` provided or `ABSURD_DOWNLOAD_EXTENSION=true`)

### Manual Extension Download

You can also download the extension separately:

```typescript
import { downloadExtension, resolveExtensionPath } from "@absurd-sqlite/bun-worker";

// Download specific version
const extensionPath = await downloadExtension({ version: "v0.1.0-alpha.3" });

// Or resolve with fallback to download
const extensionPath = await resolveExtensionPath();
```

## License

Apache-2.0

