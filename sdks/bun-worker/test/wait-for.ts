export async function waitFor(
  fn: () => void | Promise<void>,
  options?: { timeout?: number; interval?: number }
): Promise<void> {
  const timeout = options?.timeout ?? 1000;
  const interval = options?.interval ?? 10;
  const start = Date.now();

  // Poll until the expectation stops throwing or we time out.
  while (true) {
    try {
      await fn();
      return;
    } catch (err) {
      if (Date.now() - start >= timeout) {
        throw err;
      }
      await new Promise((resolve) => setTimeout(resolve, interval));
    }
  }
}
