import { existsSync, mkdirSync, writeFileSync, chmodSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { createHash } from "node:crypto";

export interface DownloadExtensionOptions {
  /**
   * Version of the extension to download. If not specified, uses "latest".
   * Examples: "v0.1.0-alpha.3", "latest"
   */
  version?: string;

  /**
   * GitHub repository owner. Defaults to "b4fun".
   */
  owner?: string;

  /**
   * GitHub repository name. Defaults to "absurd-sqlite".
   */
  repo?: string;

  /**
   * Custom cache directory for storing downloaded extensions.
   * If not specified, uses a default cache directory in user's home.
   */
  cacheDir?: string;

  /**
   * Force re-download even if cached version exists.
   */
  force?: boolean;

  /**
   * Expected SHA256 checksum of the downloaded file for validation.
   * If provided, the downloaded file's checksum will be verified against this value.
   * If the checksum doesn't match, an error will be thrown.
   */
  expectedChecksum?: string;
}

interface PlatformInfo {
  os: string;
  arch: string;
  ext: string;
}

function getPlatformInfo(): PlatformInfo {
  const platform = process.platform;
  const arch = process.arch;

  let os: string;
  let ext: string;

  switch (platform) {
    case "darwin":
      os = "macOS";
      ext = "dylib";
      break;
    case "linux":
      os = "Linux";
      ext = "so";
      break;
    case "win32":
      os = "Windows";
      ext = "dll";
      break;
    default:
      throw new Error(`Unsupported platform: ${platform}`);
  }

  let archStr: string;
  switch (arch) {
    case "x64":
      archStr = "X64";
      break;
    case "arm64":
      archStr = "ARM64";
      break;
    default:
      throw new Error(`Unsupported architecture: ${arch}`);
  }

  return { os, arch: archStr, ext };
}

function getDefaultCacheDir(): string {
  return join(homedir(), ".cache", "absurd-sqlite", "extensions");
}

function getAssetName(version: string, platform: PlatformInfo): string {
  // Format: absurd-absurd-sqlite-extension-vX.Y.Z-{OS}-{ARCH}.{ext}
  return `absurd-absurd-sqlite-extension-${version}-${platform.os}-${platform.arch}.${platform.ext}`;
}

function getTag(version: string): string {
  return `absurd-sqlite-extension/${version}`;
}

async function fetchLatestVersion(
  owner: string,
  repo: string
): Promise<string> {
  const url = `https://api.github.com/repos/${owner}/${repo}/releases`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(
      `Failed to fetch releases: ${response.status} ${response.statusText}`
    );
  }

  const releases = (await response.json()) as Array<{
    tag_name: string;
    prerelease: boolean;
    draft: boolean;
  }>;

  // Find the latest non-draft extension release
  for (const release of releases) {
    if (
      !release.draft &&
      release.tag_name.startsWith("absurd-sqlite-extension/")
    ) {
      // Extract version from tag (e.g., "absurd-sqlite-extension/v0.1.0-alpha.3" -> "v0.1.0-alpha.3")
      return release.tag_name.replace("absurd-sqlite-extension/", "");
    }
  }

  throw new Error("No extension releases found");
}

async function downloadAsset(
  owner: string,
  repo: string,
  tag: string,
  assetName: string,
  destPath: string,
  expectedChecksum?: string
): Promise<void> {
  const url = `https://github.com/${owner}/${repo}/releases/download/${tag}/${assetName}`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(
      `Failed to download extension: ${response.status} ${response.statusText} from ${url}`
    );
  }

  const buffer = await response.arrayBuffer();
  writeFileSync(destPath, Buffer.from(buffer));

  // Verify checksum if provided
  if (expectedChecksum) {
    const actualChecksum = calculateChecksum(destPath);
    if (actualChecksum !== expectedChecksum.toLowerCase()) {
      throw new Error(
        `Checksum verification failed. Expected: ${expectedChecksum.toLowerCase()}, Got: ${actualChecksum}`
      );
    }
  }

  // Make the extension executable on Unix-like systems
  if (process.platform !== "win32") {
    chmodSync(destPath, 0o755);
  }
}

function calculateChecksum(filePath: string): string {
  const fileBuffer = readFileSync(filePath);
  const hash = createHash("sha256");
  hash.update(fileBuffer);
  return hash.digest("hex");
}

function getCachedPath(
  cacheDir: string,
  version: string,
  platform: PlatformInfo
): string {
  // Use a consistent name that SQLite expects: libabsurd.{ext}
  const ext = platform.ext;
  return join(cacheDir, version, `libabsurd.${ext}`);
}

/**
 * Downloads the absurd-sqlite extension from GitHub releases.
 * Returns the path to the downloaded extension file.
 *
 * @param options - Download options
 * @returns Path to the extension file
 *
 * @example
 * ```typescript
 * import { downloadExtension } from "@absurd-sqlite/sdk";
 *
 * // Download latest version
 * const extensionPath = await downloadExtension();
 *
 * // Download specific version
 * const extensionPath = await downloadExtension({ version: "v0.1.0-alpha.3" });
 *
 * // Download with checksum verification
 * const extensionPath = await downloadExtension({
 *   version: "v0.1.0-alpha.3",
 *   expectedChecksum: "abc123..."
 * });
 * ```
 */
export async function downloadExtension(
  options: DownloadExtensionOptions = {}
): Promise<string> {
  const owner = options.owner ?? "b4fun";
  const repo = options.repo ?? "absurd-sqlite";
  const cacheDir = options.cacheDir ?? getDefaultCacheDir();
  const force = options.force ?? false;

  // Resolve version
  let version = options.version ?? "latest";
  if (version === "latest") {
    version = await fetchLatestVersion(owner, repo);
  }

  // Get platform info
  const platform = getPlatformInfo();
  const assetName = getAssetName(version, platform);
  const tag = getTag(version);

  // Check cache
  const cachedPath = getCachedPath(cacheDir, version, platform);

  if (!force && existsSync(cachedPath)) {
    // If checksum is provided, verify cached file
    if (options.expectedChecksum) {
      const actualChecksum = calculateChecksum(cachedPath);
      if (actualChecksum !== options.expectedChecksum.toLowerCase()) {
        throw new Error(
          `Cached file checksum verification failed. Expected: ${options.expectedChecksum.toLowerCase()}, Got: ${actualChecksum}`
        );
      }
    }
    return cachedPath;
  }

  // Ensure cache directory exists
  const versionDir = join(cacheDir, version);
  mkdirSync(versionDir, { recursive: true });

  // Download asset
  await downloadAsset(owner, repo, tag, assetName, cachedPath, options.expectedChecksum);

  return cachedPath;
}

/**
 * Resolves the extension path, either from the provided path, environment variable,
 * or by downloading from GitHub releases.
 *
 * @param extensionPath - Optional path to the extension file
 * @param downloadOptions - Options for downloading if no path is provided
 * @returns Path to the extension file
 *
 * @example
 * ```typescript
 * import { resolveExtensionPath } from "@absurd-sqlite/sdk";
 *
 * // Use provided path
 * const path1 = await resolveExtensionPath("/path/to/extension.so");
 *
 * // Use environment variable or download
 * const path2 = await resolveExtensionPath();
 * ```
 */
export async function resolveExtensionPath(
  extensionPath?: string,
  downloadOptions?: DownloadExtensionOptions
): Promise<string> {
  // If path is provided, use it
  if (extensionPath) {
    return extensionPath;
  }

  // Try environment variable
  const envPath = process.env.ABSURD_SQLITE_EXTENSION_PATH;
  if (envPath) {
    return envPath;
  }

  // Download from GitHub releases
  return downloadExtension(downloadOptions);
}
