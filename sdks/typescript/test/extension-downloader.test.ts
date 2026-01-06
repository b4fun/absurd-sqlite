import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  downloadExtension,
  resolveExtensionPath,
} from "../src/extension-downloader";

let tempCacheDir: string | null = null;

afterEach(() => {
  if (tempCacheDir) {
    rmSync(tempCacheDir, { recursive: true, force: true });
    tempCacheDir = null;
  }
});

describe("downloadExtension", () => {
  it("downloads specific version", async () => {
    tempCacheDir = mkdtempSync(join(tmpdir(), "absurd-ext-cache-"));

    const extensionPath = await downloadExtension({
      version: "v0.1.0-alpha.3",
      cacheDir: tempCacheDir,
    });

    expect(existsSync(extensionPath)).toBe(true);
    expect(extensionPath).toContain("v0.1.0-alpha.3");

    // Verify it has the right extension
    const platform = process.platform;
    if (platform === "darwin") {
      expect(extensionPath).toMatch(/\.dylib$/);
    } else if (platform === "linux") {
      expect(extensionPath).toMatch(/\.so$/);
    } else if (platform === "win32") {
      expect(extensionPath).toMatch(/\.dll$/);
    }
  }, 60000);

  it("uses cached version on second call", async () => {
    tempCacheDir = mkdtempSync(join(tmpdir(), "absurd-ext-cache-"));

    const path1 = await downloadExtension({
      version: "v0.1.0-alpha.3",
      cacheDir: tempCacheDir,
    });

    const path2 = await downloadExtension({
      version: "v0.1.0-alpha.3",
      cacheDir: tempCacheDir,
    });

    expect(path1).toBe(path2);
  }, 60000);

  it("forces re-download when force=true", async () => {
    tempCacheDir = mkdtempSync(join(tmpdir(), "absurd-ext-cache-"));

    const path1 = await downloadExtension({
      version: "v0.1.0-alpha.3",
      cacheDir: tempCacheDir,
    });

    // Delete the cached file
    rmSync(path1);
    expect(existsSync(path1)).toBe(false);

    // Download again with force
    const path2 = await downloadExtension({
      version: "v0.1.0-alpha.3",
      cacheDir: tempCacheDir,
      force: true,
    });

    expect(existsSync(path2)).toBe(true);
    expect(path2).toBe(path1); // Same path
  }, 60000);
});

describe("resolveExtensionPath", () => {
  it("returns provided path when given", async () => {
    const providedPath = "/custom/path/to/extension.so";
    const resolved = await resolveExtensionPath(providedPath);
    expect(resolved).toBe(providedPath);
  });

  it("uses environment variable when no path provided", async () => {
    const envPath = "/env/path/to/extension.so";
    const originalEnv = process.env.ABSURD_SQLITE_EXTENSION_PATH;

    try {
      process.env.ABSURD_SQLITE_EXTENSION_PATH = envPath;
      const resolved = await resolveExtensionPath();
      expect(resolved).toBe(envPath);
    } finally {
      if (originalEnv !== undefined) {
        process.env.ABSURD_SQLITE_EXTENSION_PATH = originalEnv;
      } else {
        delete process.env.ABSURD_SQLITE_EXTENSION_PATH;
      }
    }
  });

  it("downloads when no path or env var provided", async () => {
    tempCacheDir = mkdtempSync(join(tmpdir(), "absurd-ext-cache-"));
    const originalEnv = process.env.ABSURD_SQLITE_EXTENSION_PATH;

    try {
      delete process.env.ABSURD_SQLITE_EXTENSION_PATH;

      const resolved = await resolveExtensionPath(undefined, {
        version: "v0.1.0-alpha.3",
        cacheDir: tempCacheDir,
      });

      expect(existsSync(resolved)).toBe(true);
      expect(resolved).toContain(tempCacheDir);
    } finally {
      if (originalEnv !== undefined) {
        process.env.ABSURD_SQLITE_EXTENSION_PATH = originalEnv;
      }
    }
  }, 60000);
});
