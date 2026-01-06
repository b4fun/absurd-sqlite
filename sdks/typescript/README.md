# Absurd SQLite SDK for TypeScript

TypeScript SDK for Absurd-SQLite - SQLite-based durable task execution.

## Installation

```bash
npm install @absurd-sqlite/sdk better-sqlite3
```

## Usage

### Basic Setup

```typescript
import Database from "better-sqlite3";
import { Absurd } from "@absurd-sqlite/sdk";

const db = new Database("mydb.db");

// Option 1: Provide extension path manually
const absurd = new Absurd(db, "/path/to/libabsurd.so");

// Option 2: Use Absurd.create() for automatic extension resolution
const absurd = await Absurd.create(db);

await absurd.createQueue("default");
```

### Automatic Extension Download

The SDK can automatically download the appropriate extension for your platform from GitHub releases:

```typescript
import Database from "better-sqlite3";
import { Absurd } from "@absurd-sqlite/sdk";

const db = new Database("mydb.db");

// Download and use the latest extension
const absurd = await Absurd.create(db);

// Or specify a version
const absurd = await Absurd.create(db, {
  downloadOptions: { version: "v0.1.0-alpha.3" }
});
```

The extension is cached in `~/.cache/absurd-sqlite/extensions/` to avoid repeated downloads.

### Extension Resolution Order

When using `Absurd.create()`, the extension path is resolved in the following order:

1. Explicitly provided `extensionPath` in options
2. `ABSURD_SQLITE_EXTENSION_PATH` environment variable
3. Automatic download from GitHub releases (latest or specified version)

### Manual Extension Download

You can also download the extension separately:

```typescript
import { downloadExtension, resolveExtensionPath } from "@absurd-sqlite/sdk";

// Download specific version
const extensionPath = await downloadExtension({ version: "v0.1.0-alpha.3" });

// Or resolve with fallback to download
const extensionPath = await resolveExtensionPath();
```

## API Documentation

For full API documentation, see the [TypeDoc generated documentation](https://b4fun.github.io/absurd-sqlite).

## License

Apache-2.0

