/**
 * Temporal API types for representing dates/times and durations.
 *
 * This module re-exports the Temporal types from temporal-polyfill and provides
 * helper functions for working with them in the context of SQLite.
 *
 * @see https://tc39.es/proposal-temporal/docs/
 * @see https://github.com/fullcalendar/temporal-polyfill
 */

import { Temporal } from "temporal-polyfill";

// Re-export the Temporal namespace for use with this SDK
export { Temporal };

/**
 * Represents an exact point in time, independent of timezone.
 * This is a re-export of Temporal.Instant from temporal-polyfill.
 */
export type Instant = Temporal.Instant;

/**
 * Represents a duration of time.
 * This is a re-export of Temporal.Duration from temporal-polyfill.
 */
export type Duration = Temporal.Duration;

/**
 * Creates a Duration from seconds.
 * @param seconds Number of seconds
 * @returns A Temporal.Duration representing the given seconds
 */
export function durationFromSeconds(seconds: number): Duration {
  return Temporal.Duration.from({ seconds });
}

/**
 * Converts a Duration to total seconds.
 * @param duration The duration to convert
 * @returns Total seconds (may be fractional for sub-second durations)
 */
export function durationToSeconds(duration: Duration): number {
  return duration.total({ unit: "second" });
}

/**
 * Creates an Instant from a Date object.
 * @param date JavaScript Date object
 * @returns A Temporal.Instant
 */
export function instantFromDate(date: Date): Instant {
  return Temporal.Instant.fromEpochMilliseconds(date.getTime());
}

/**
 * Creates an Instant from epoch milliseconds.
 * @param epochMs Unix timestamp in milliseconds
 * @returns A Temporal.Instant
 */
export function instantFromEpochMilliseconds(epochMs: number): Instant {
  return Temporal.Instant.fromEpochMilliseconds(epochMs);
}

/**
 * Converts an Instant to a Date object.
 * @param instant The instant to convert
 * @returns A JavaScript Date object
 */
export function instantToDate(instant: Instant): Date {
  return new Date(instant.epochMilliseconds);
}

/**
 * Converts an Instant to epoch milliseconds.
 * @param instant The instant to convert
 * @returns Unix timestamp in milliseconds
 */
export function instantToEpochMilliseconds(instant: Instant): number {
  return instant.epochMilliseconds;
}

/**
 * Type guard to check if a value is a Temporal.Instant.
 * @param value The value to check
 * @returns True if the value is a Temporal.Instant
 */
export function isInstant(value: unknown): value is Instant {
  return (
    value !== null &&
    typeof value === "object" &&
    value instanceof Temporal.Instant
  );
}

/**
 * Type guard to check if a value is a Temporal.Duration.
 * @param value The value to check
 * @returns True if the value is a Temporal.Duration
 */
export function isDuration(value: unknown): value is Duration {
  return (
    value !== null &&
    typeof value === "object" &&
    value instanceof Temporal.Duration
  );
}
