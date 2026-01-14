import { describe, test, expect } from "vitest";
import {
  Temporal,
  durationFromSeconds,
  durationToSeconds,
  instantFromDate,
  instantFromEpochMilliseconds,
  instantToDate,
  instantToEpochMilliseconds,
  isInstant,
  isDuration,
} from "../src/temporal-types";

describe("Temporal types", () => {
  describe("Duration helpers", () => {
    test("durationFromSeconds creates a Duration from seconds", () => {
      const duration = durationFromSeconds(120);
      expect(isDuration(duration)).toBe(true);
      expect(duration.seconds).toBe(120);
    });

    test("durationToSeconds converts Duration to total seconds", () => {
      const duration = Temporal.Duration.from({ minutes: 2, seconds: 30 });
      expect(durationToSeconds(duration)).toBe(150);
    });

    test("durationFromSeconds and durationToSeconds are inverse operations", () => {
      const original = 3661; // 1 hour, 1 minute, 1 second
      const duration = durationFromSeconds(original);
      expect(durationToSeconds(duration)).toBe(original);
    });
  });

  describe("Instant helpers", () => {
    test("instantFromDate creates an Instant from a Date", () => {
      const date = new Date("2024-05-01T12:00:00.000Z");
      const instant = instantFromDate(date);
      expect(isInstant(instant)).toBe(true);
      expect(instant.epochMilliseconds).toBe(date.getTime());
    });

    test("instantToDate converts an Instant back to Date", () => {
      const instant = Temporal.Instant.from("2024-05-01T12:00:00Z");
      const date = instantToDate(instant);
      expect(date).toBeInstanceOf(Date);
      expect(date.toISOString()).toBe("2024-05-01T12:00:00.000Z");
    });

    test("instantFromDate and instantToDate are inverse operations", () => {
      const originalDate = new Date("2024-06-15T08:30:45.123Z");
      const instant = instantFromDate(originalDate);
      const roundTripped = instantToDate(instant);
      expect(roundTripped.getTime()).toBe(originalDate.getTime());
    });

    test("instantFromEpochMilliseconds creates Instant from epoch ms", () => {
      const epochMs = 1714564800000; // 2024-05-01T12:00:00Z
      const instant = instantFromEpochMilliseconds(epochMs);
      expect(isInstant(instant)).toBe(true);
      expect(instant.epochMilliseconds).toBe(epochMs);
    });

    test("instantToEpochMilliseconds extracts epoch ms from Instant", () => {
      const instant = Temporal.Instant.from("2024-05-01T12:00:00Z");
      const epochMs = instantToEpochMilliseconds(instant);
      expect(epochMs).toBe(1714564800000);
    });
  });

  describe("Type guards", () => {
    test("isInstant returns true for Temporal.Instant", () => {
      const instant = Temporal.Instant.from("2024-01-01T00:00:00Z");
      expect(isInstant(instant)).toBe(true);
    });

    test("isInstant returns false for Date", () => {
      const date = new Date();
      expect(isInstant(date)).toBe(false);
    });

    test("isInstant returns false for other types", () => {
      expect(isInstant(null)).toBe(false);
      expect(isInstant(undefined)).toBe(false);
      expect(isInstant(12345)).toBe(false);
      expect(isInstant("2024-01-01")).toBe(false);
      expect(isInstant({})).toBe(false);
    });

    test("isDuration returns true for Temporal.Duration", () => {
      const duration = Temporal.Duration.from({ hours: 1 });
      expect(isDuration(duration)).toBe(true);
    });

    test("isDuration returns false for numbers", () => {
      expect(isDuration(3600)).toBe(false);
    });

    test("isDuration returns false for other types", () => {
      expect(isDuration(null)).toBe(false);
      expect(isDuration(undefined)).toBe(false);
      expect(isDuration("PT1H")).toBe(false);
      expect(isDuration({})).toBe(false);
    });
  });

  describe("Temporal.Duration ISO string parsing", () => {
    test("parses ISO 8601 duration strings", () => {
      const duration = Temporal.Duration.from("PT1H30M");
      expect(duration.hours).toBe(1);
      expect(duration.minutes).toBe(30);
    });
  });

  describe("Temporal.Instant ISO string parsing", () => {
    test("parses ISO 8601 instant strings", () => {
      const instant = Temporal.Instant.from("2024-05-01T12:30:45.123Z");
      expect(instant.toString()).toBe("2024-05-01T12:30:45.123Z");
    });
  });
});
