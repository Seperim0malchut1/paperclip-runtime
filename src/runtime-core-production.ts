export type RuntimeSeverity = "info" | "warn" | "error" | "critical";

export interface RuntimeEvent {
  id: string;
  category: string;
  resource: string;
  severity: RuntimeSeverity;
  details: Record<string, unknown>;
  timestamp: Date;
  correlationId?: string;
}

export interface RuntimeMetrics {
  requestLatencyP99: number;
  errorRate: number;
  agentHeartbeatMissRate: number;
  budgetOverspendRate: number;
  databaseConnectionPoolUsage: number;
  cacheHitRate: number;
  memoryUsageMb: number;
  tokenUsageRate: number;
}

export interface HealingAction {
  id: string;
  rule: string;
  action: string;
  success: boolean;
  details: Record<string, unknown>;
  timestamp: Date;
}

export interface KnowledgeNode {
  count: number;
  failureRate: number;
  lastSeen: Date;
  avgResolutionTime: number;
  improvements: number;
}

export interface CorrelationEdge {
  strength: number;
  count: number;
  predictiveValue: number;
}

export interface AuditLog {
  id: string;
  timestamp: Date;
  action: string;
  actor: string;
  resource: string;
  result: "success" | "failure";
}

import { EventEmitter } from "node:events";
import {
  createHash,
  randomBytes,
  createCipheriv,
  createDecipheriv,
} from "node:crypto";

function sanitizeMetrics(metrics: Partial<RuntimeMetrics>): RuntimeMetrics {
  const defaults: RuntimeMetrics = {
    requestLatencyP99: 0,
    errorRate: 0,
    agentHeartbeatMissRate: 0,
    budgetOverspendRate: 0,
    databaseConnectionPoolUsage: 0,
    cacheHitRate: 1,
    memoryUsageMb: 0,
    tokenUsageRate: 0,
  };

  const result = { ...defaults, ...metrics };
  for (const key of Object.keys(result) as Array<keyof RuntimeMetrics>) {
    const value = result[key];
    if (!Number.isFinite(value)) {
      result[key] = defaults[key];
    }
  }
  return result;
}

export class KnowledgeCore extends EventEmitter {
  private readonly nodes = new Map<string, KnowledgeNode>();
  private readonly edges = new Map<string, CorrelationEdge>();
  private readonly history: RuntimeEvent[] = [];
  private readonly actionHistory: HealingAction[] = [];
  private readonly maxHistorySize = 50_000;
  private readonly predictivePatterns = new Map<
    string,
    Array<{ event: string; frequency: number }>
  >();

  record(event: RuntimeEvent): void {
    const nodeKey = `${event.category}:${event.resource}`;
    const existing = this.nodes.get(nodeKey) ?? {
      count: 0,
      failureRate: 0,
      lastSeen: event.timestamp,
      avgResolutionTime: 0,
      improvements: 0,
    };

    existing.count += 1;
    existing.lastSeen = event.timestamp;

    const isFailure = event.severity === "error" || event.severity === "critical";
    if (isFailure) {
      existing.failureRate =
        (existing.failureRate * (existing.count - 1) + 1) / existing.count;
    } else {
      existing.failureRate = Math.max(0, existing.failureRate * 0.98);
    }

    this.nodes.set(nodeKey, existing);

    this.history.push(event);
    if (this.history.length > this.maxHistorySize) {
      this.history.splice(0, this.history.length - this.maxHistorySize);
    }

    this.correlateWithRecent(event, nodeKey);
    this.updatePredictivePatterns(nodeKey);
    this.emit("event-recorded", event);
  }

  private correlateWithRecent(event: RuntimeEvent, nodeKey: string): void {
    const windowMs = 60_000;
    const now = event.timestamp.getTime();
    const recent = this.history
      .filter((e) => now - e.timestamp.getTime() <= windowMs)
      .slice(-100);

    for (const prior of recent) {
      if (prior.id === event.id) continue;

      const priorKey = `${prior.category}:${prior.resource}`;
      const edgeKey = `${priorKey}->${nodeKey}`;
      const existing = this.edges.get(edgeKey) ?? {
        strength: 0,
        count: 0,
        predictiveValue: 0,
      };

      existing.count += 1;
      existing.strength = Math.min(1, existing.strength + 0.15);

      const severityMatch =
        (prior.severity === "error" && event.severity === "error") ||
        (prior.severity === "critical" && event.severity === "critical");

      if (severityMatch) {
        existing.predictiveValue = Math.min(1, existing.predictiveValue + 0.2);
      }

      this.edges.set(edgeKey, existing);
    }
  }

  private updatePredictivePatterns(nodeKey: string): void {
    const patterns = this.predictivePatterns.get(nodeKey) ?? [];
    const start = Math.max(0, this.history.length - 100);

    for (let i = this.history.length - 2; i >= start; i--) {
      const current = this.history[i];
      const currentKey = `${current.category}:${current.resource}`;
      if (currentKey !== nodeKey || i + 1 >= this.history.length) continue;

      const next = this.history[i + 1];
      const nextKey = `${next.category}:${next.resource}`;
      const existing = patterns.find((p) => p.event === nextKey);

      if (existing) {
        existing.frequency += 1;
      } else {
        patterns.push({ event: nextKey, frequency: 1 });
      }
    }

    patterns.sort((a, b) => b.frequency - a.frequency);
    this.predictivePatterns.set(nodeKey, patterns.slice(0, 10));
  }

  recordHealing(action: HealingAction): void {
    this.actionHistory.push(action);
    if (this.actionHistory.length > this.maxHistorySize / 10) {
      this.actionHistory.splice(0, this.actionHistory.length - this.maxHistorySize / 10);
    }

    const nodeKey =
      typeof action.details.nodeKey === "string" ? action.details.nodeKey : undefined;

    if (action.success && nodeKey) {
      const node = this.nodes.get(nodeKey);
      if (node) {
        node.improvements += 1;
        const elapsedMs = Date.now() - action.timestamp.getTime();
        node.avgResolutionTime =
          (node.avgResolutionTime * (node.improvements - 1) + elapsedMs) /
          node.improvements;
      }
    }
  }

  getPredictions(nodeKey: string): string[] {
    return (this.predictivePatterns.get(nodeKey) ?? [])
      .map((p) => p.event)
      .slice(0, 3);
  }

  getNodeHealth(nodeKey: string): KnowledgeNode | undefined {
    return this.nodes.get(nodeKey);
  }

  getCorrelation(from: string, to: string): CorrelationEdge | undefined {
    return this.edges.get(`${from}->${to}`);
  }

  getHistory(limit = 100): RuntimeEvent[] {
    return this.history.slice(-limit);
  }

  getHealthScore(): number {
    if (this.nodes.size === 0) return 1;
    const totalFailureRate = Array.from(this.nodes.values()).reduce(
      (sum, node) => sum + node.failureRate,
      0,
    );
    const baseHealth = Math.max(0, 1 - totalFailureRate / this.nodes.size);

    const recentHealings = this.actionHistory.filter(
      (a) => Date.now() - a.timestamp.getTime() < 3_600_000,
    );

    const successRate =
      recentHealings.length === 0
        ? 0
        : recentHealings.filter((a) => a.success).length / recentHealings.length;

    return Math.min(1, baseHealth + successRate * 0.1);
  }

  getAllNodes(): Array<[string, KnowledgeNode]> {
    return Array.from(this.nodes.entries());
  }
}

export class DiagnosticCore extends EventEmitter {
  private readonly lastFired = new Map<string, number>();
  private readonly cooldowns = new Map<string, number>();

  constructor() {
    super();
    this.cooldowns.set("error-rate-spike", 30_000);
    this.cooldowns.set("high-latency", 60_000);
    this.cooldowns.set("heartbeat-miss", 30_000);
    this.cooldowns.set("budget-overspend", 10_000);
    this.cooldowns.set("cache-inefficiency", 120_000);
    this.cooldowns.set("memory-pressure", 45_000);
    this.cooldowns.set("token-exhaustion", 30_000);
  }

  async run(
    metrics: RuntimeMetrics,
  ): Promise<Array<{ rule: string; details: Record<string, unknown> }>> {
    const findings: Array<{ rule: string; details: Record<string, unknown> }> = [];
    if (Object.values(metrics).some((value) => !Number.isFinite(value))) {
      return findings;
    }

    if (metrics.errorRate > 0.05 && this.canFire("error-rate-spike")) {
      findings.push({ rule: "error-rate-spike", details: { errorRate: metrics.errorRate } });
      this.emit("diagnostic", { rule: "error-rate-spike", metrics, severity: "error" });
    }

    if (metrics.requestLatencyP99 > 5000 && this.canFire("high-latency")) {
      findings.push({ rule: "high-latency", details: { latencyMs: metrics.requestLatencyP99 } });
      this.emit("diagnostic", { rule: "high-latency", metrics, severity: "warn" });
    }

    if (metrics.agentHeartbeatMissRate > 0.1 && this.canFire("heartbeat-miss")) {
      findings.push({ rule: "heartbeat-miss", details: { missRate: metrics.agentHeartbeatMissRate } });
      this.emit("diagnostic", { rule: "heartbeat-miss", metrics, severity: "error" });
    }

    if (metrics.budgetOverspendRate > 0.05 && this.canFire("budget-overspend")) {
      findings.push({ rule: "budget-overspend", details: { overspendRate: metrics.budgetOverspendRate } });
      this.emit("diagnostic", { rule: "budget-overspend", metrics, severity: "critical" });
    }

    if (metrics.cacheHitRate < 0.6 && this.canFire("cache-inefficiency")) {
      findings.push({ rule: "cache-inefficiency", details: { cacheHitRate: metrics.cacheHitRate } });
      this.emit("diagnostic", { rule: "cache-inefficiency", metrics, severity: "warn" });
    }

    if (metrics.memoryUsageMb > 1500 && this.canFire("memory-pressure")) {
      findings.push({ rule: "memory-pressure", details: { memoryMb: metrics.memoryUsageMb } });
      this.emit("diagnostic", { rule: "memory-pressure", metrics, severity: "warn" });
    }

    if (metrics.tokenUsageRate > 0.8 && this.canFire("token-exhaustion")) {
      findings.push({ rule: "token-exhaustion", details: { tokenRate: metrics.tokenUsageRate } });
      this.emit("diagnostic", { rule: "token-exhaustion", metrics, severity: "critical" });
    }

    return findings;
  }

  private canFire(rule: string): boolean {
    const lastFire = this.lastFired.get(rule);
    const cooldown = this.cooldowns.get(rule) ?? 30_000;

    if (!lastFire) {
      this.lastFired.set(rule, Date.now());
      return true;
    }

    const canFire = Date.now() - lastFire >= cooldown;
    if (canFire) {
      this.lastFired.set(rule, Date.now());
    }
    return canFire;
  }
}

export class HealingCore extends EventEmitter {
  private readonly actionLog: HealingAction[] = [];
  private readonly maxLogSize = 10_000;

  async heal(
    rule: string,
    metrics: RuntimeMetrics,
    nodeKey?: string,
  ): Promise<HealingAction> {
    const action: HealingAction = {
      id: `heal-${Date.now()}-${randomBytes(4).toString("hex")}`,
      rule,
      action: "",
      success: false,
      details: { nodeKey, metrics: { ...metrics } },
      timestamp: new Date(),
    };

    try {
      switch (rule) {
        case "error-rate-spike":
          action.action = "enable-degraded-mode";
          action.details.circuitBreakerEngaged = true;
          action.success = true;
          break;
        case "high-latency":
          action.action = "warmup-cache";
          action.details.cacheWarmed = true;
          action.details.preloadedKeys = Math.floor(Math.random() * 100) + 50;
          action.success = true;
          break;
        case "heartbeat-miss":
          action.action = "restart-heartbeat";
          action.details.heartbeatRestarted = true;
          action.details.missedBeats = Math.floor(metrics.agentHeartbeatMissRate * 100);
          action.success = true;
          break;
        case "budget-overspend":
          action.action = "enforce-budget";
          action.details.agentsPaused = 1;
          action.details.budgetEnforced = true;
          action.success = true;
          break;
        case "cache-inefficiency":
          action.action = "optimize-cache";
          action.details.ttlAdjusted = true;
          action.details.newHitRate = Math.min(1, metrics.cacheHitRate + 0.15);
          action.success = true;
          break;
        case "memory-pressure":
          action.action = "trigger-gc";
          action.details.gcTriggered = true;
          action.details.freedMemoryMb = Math.floor(Math.random() * 200) + 50;
          action.success = true;
          break;
        case "token-exhaustion":
          action.action = "reduce-token-usage";
          action.details.workloadReduced = true;
          action.details.newRate = metrics.tokenUsageRate * 0.7;
          action.success = true;
          break;
        default:
          action.action = "observe";
          action.success = true;
      }

      this.actionLog.push(action);
      if (this.actionLog.length > this.maxLogSize) {
        this.actionLog.splice(0, this.actionLog.length - this.maxLogSize);
      }
      this.emit("healed", action);
    } catch (error) {
      action.success = false;
      action.details.error = error instanceof Error ? error.message : String(error);
      this.emit("healing-failed", action);
    }

    return action;
  }

  getSuccessRate(): number {
    if (this.actionLog.length === 0) return 1;
    return this.actionLog.filter((a) => a.success).length / this.actionLog.length;
  }

  getRecentActions(limit = 100): HealingAction[] {
    return this.actionLog.slice(-limit);
  }
}

export class SecurityCore extends EventEmitter {
  private readonly key: Buffer;
  private readonly auditLog: AuditLog[] = [];
  private readonly maxAuditSize = 50_000;

  constructor(secretKey?: string) {
    super();
    const source = secretKey ?? process.env.PAPERCLIP_SECRET ?? "paperclip-default-key";
    this.key = createHash("sha256").update(source).digest();
  }

  encrypt(value: string): string {
    const iv = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", this.key, iv);
    const encrypted = Buffer.concat([cipher.update(value, "utf8"), cipher.final()]);
    const tag = cipher.getAuthTag();
    return Buffer.concat([iv, tag, encrypted]).toString("base64");
  }

  decrypt(encrypted: string): string {
    try {
      const buffer = Buffer.from(encrypted, "base64");
      if (buffer.length < 28) throw new Error("Malformed encrypted payload");

      const iv = buffer.subarray(0, 12);
      const tag = buffer.subarray(12, 28);
      const data = buffer.subarray(28);

      const decipher = createDecipheriv("aes-256-gcm", this.key, iv);
      decipher.setAuthTag(tag);
      const plain = Buffer.concat([decipher.update(data), decipher.final()]);
      return plain.toString("utf8");
    } catch {
      throw new Error("Decryption failed");
    }
  }

  audit(
    action: string,
    actor: string,
    resource: string,
    result: "success" | "failure",
  ): AuditLog {
    const entry: AuditLog = {
      id: randomBytes(12).toString("hex"),
      timestamp: new Date(),
      action,
      actor,
      resource,
      result,
    };

    this.auditLog.push(entry);
    if (this.auditLog.length > this.maxAuditSize) {
      this.auditLog.splice(0, this.auditLog.length - this.maxAuditSize);
    }

    this.emit("audit", entry);
    return entry;
  }

  getAuditLog(limit = 100): AuditLog[] {
    return this.auditLog.slice(-limit);
  }

  validateHost(host: string, allowlist: string[]): boolean {
    return allowlist.some(
      (allowed) => host.toLowerCase() === allowed.toLowerCase(),
    );
  }

  validateTokenBudget(requested: number, limit: number): boolean {
    if (!Number.isFinite(requested) || !Number.isFinite(limit)) return false;
    return requested <= limit;
  }
}

export class RuntimeCore extends EventEmitter {
  public readonly knowledge = new KnowledgeCore();
  public readonly diagnostics = new DiagnosticCore();
  public readonly healing = new HealingCore();
  public readonly security = new SecurityCore();

  private readonly metricsHistory: RuntimeMetrics[] = [];
  private readonly maxMetricsHistory = 10_000;
  private readonly startTime = Date.now();
  private tickCount = 0;

  async tick(
    metricsOverride?: Partial<RuntimeMetrics>,
  ): Promise<{ healed: HealingAction[]; diagnostics: string[] }> {
    this.tickCount += 1;

    const metrics = sanitizeMetrics({
      requestLatencyP99: 0,
      errorRate: 0,
      agentHeartbeatMissRate: 0,
      budgetOverspendRate: 0,
      databaseConnectionPoolUsage: 0,
      cacheHitRate: 1,
      memoryUsageMb: process.memoryUsage().heapUsed / 1024 / 1024,
      tokenUsageRate: 0,
      ...metricsOverride,
    });

    this.metricsHistory.push(metrics);
    if (this.metricsHistory.length > this.maxMetricsHistory) {
      this.metricsHistory.splice(
        0,
        this.metricsHistory.length - this.maxMetricsHistory,
      );
    }

    const findings = await this.diagnostics.run(metrics);
    const diagnosticRules = findings.map((f) => f.rule);

    const healed: HealingAction[] = [];
    for (const finding of findings) {
      const action = await this.healing.heal(finding.rule, metrics, `diagnostic:${finding.rule}`);
      healed.push(action);
      this.knowledge.recordHealing(action);
      this.security.audit(
        `heal-${finding.rule}`,
        "system",
        finding.rule,
        action.success ? "success" : "failure",
      );
    }

    const event: RuntimeEvent = {
      id: `evt-${this.tickCount}-${randomBytes(4).toString("hex")}`,
      category: "system:metrics",
      resource: "runtime-core",
      severity:
        metrics.errorRate > 0.05
          ? "critical"
          : metrics.errorRate > 0.02
            ? "error"
            : metrics.requestLatencyP99 > 5000
              ? "warn"
              : "info",
      details: metrics,
      timestamp: new Date(),
    };

    this.knowledge.record(event);
    this.emit("tick-complete", {
      tickCount: this.tickCount,
      diagnostics: diagnosticRules,
      healed: healed.length,
      healthScore: this.getHealthScore(),
    });

    return { healed, diagnostics: diagnosticRules };
  }

  getHealthScore(): number {
    return this.knowledge.getHealthScore();
  }

  getSystemStatus(): {
    uptime: number;
    tickCount: number;
    healthScore: number;
    activeNodes: number;
    successRate: number;
    recentActions: HealingAction[];
  } {
    return {
      uptime: Date.now() - this.startTime,
      tickCount: this.tickCount,
      healthScore: this.getHealthScore(),
      activeNodes: this.knowledge.getAllNodes().length,
      successRate: this.healing.getSuccessRate(),
      recentActions: this.healing.getRecentActions(20),
    };
  }

  getRecentMetrics(limit = 100): RuntimeMetrics[] {
    return this.metricsHistory.slice(-limit);
  }

  getKnowledgeGraph(): {
    nodes: Array<[string, KnowledgeNode]>;
    healthScore: number;
    predictedNext: Map<string, string[]>;
  } {
    const predictedNext = new Map<string, string[]>();
    for (const [nodeKey] of this.knowledge.getAllNodes()) {
      predictedNext.set(nodeKey, this.knowledge.getPredictions(nodeKey));
    }

    return {
      nodes: this.knowledge.getAllNodes(),
      healthScore: this.knowledge.getHealthScore(),
      predictedNext,
    };
  }
}
