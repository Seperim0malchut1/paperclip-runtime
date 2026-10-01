import { describe, it, expect } from "vitest";
import { RuntimeCore, SecurityCore, DiagnosticCore, HealingCore, KnowledgeCore } from "../src/runtime-core-production";

describe("SecurityCore", () => {
  it("encrypts and decrypts a payload", () => {
    const sec = new SecurityCore("secret-key");
    const input = "super-sensitive-token";
    const cipher = sec.encrypt(input);
    expect(cipher).not.toBe(input);
    expect(sec.decrypt(cipher)).toBe(input);
  });

  it("validates hosts and budgets", () => {
    const sec = new SecurityCore("secret-key");
    expect(sec.validateHost("api.example.com", ["api.example.com"])).toBe(true);
    expect(sec.validateHost("evil.example.com", ["api.example.com"])).toBe(false);
    expect(sec.validateTokenBudget(100, 200)).toBe(true);
    expect(sec.validateTokenBudget(500, 200)).toBe(false);
  });
});

describe("DiagnosticCore", () => {
  it("fires diagnostics when thresholds are crossed", async () => {
    const diag = new DiagnosticCore();
    const findings = await diag.run({
      requestLatencyP99: 6000,
      errorRate: 0.2,
      agentHeartbeatMissRate: 0.2,
      budgetOverspendRate: 0.2,
      databaseConnectionPoolUsage: 0.8,
      cacheHitRate: 0.2,
      memoryUsageMb: 2000,
      tokenUsageRate: 0.9,
    });

    expect(findings.length).toBeGreaterThan(0);
  });
});

describe("HealingCore", () => {
  it("creates a successful healing action", async () => {
    const healer = new HealingCore();
    const result = await healer.heal("high-latency", {
      requestLatencyP99: 7000,
      errorRate: 0.03,
      agentHeartbeatMissRate: 0,
      budgetOverspendRate: 0,
      databaseConnectionPoolUsage: 0,
      cacheHitRate: 0.4,
      memoryUsageMb: 1200,
      tokenUsageRate: 0.2,
    });

    expect(result.success).toBe(true);
    expect(result.action).toBe("warmup-cache");
  });
});

describe("KnowledgeCore", () => {
  it("tracks events and health", () => {
    const knowledge = new KnowledgeCore();
    knowledge.record({
      id: "evt-1",
      category: "system:metrics",
      resource: "runtime-core",
      severity: "error",
      details: { errorRate: 0.2 },
      timestamp: new Date(),
    });

    expect(knowledge.getNodeHealth("system:metrics:runtime-core")).toBeDefined();
    expect(knowledge.getHealthScore()).toBeGreaterThanOrEqual(0);
  });
});

describe("RuntimeCore", () => {
  it("runs a tick with healing and reports status", async () => {
    const runtime = new RuntimeCore();
    const result = await runtime.tick({
      requestLatencyP99: 7000,
      errorRate: 0.08,
      agentHeartbeatMissRate: 0.2,
      budgetOverspendRate: 0.07,
      databaseConnectionPoolUsage: 0.8,
      cacheHitRate: 0.45,
      memoryUsageMb: 1700,
      tokenUsageRate: 0.9,
    });

    expect(result.healed.length).toBeGreaterThan(0);
    expect(result.diagnostics.length).toBeGreaterThan(0);
    expect(runtime.getSystemStatus().tickCount).toBe(1);
  });
});
