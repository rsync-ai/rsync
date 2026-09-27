/**
 * The Steps/DAG tab's live view for a CDC pipeline.
 *
 * The tab renders `pipeline_progress.metadata.execution_plan`, which the Temporal
 * workflow builds ONCE before any data moves — eight agent setup stages. For a
 * stream the workflow completes `executor`, marks the execution `streaming_active`
 * and returns, writing no further stage. So the tab was a frozen picture of a
 * setup that finished hours ago: every node green, the "Live" badge never shown
 * (it needs a `running` stage), nothing about the data actually moving.
 *
 * `buildLiveStreamStages` is the replacement's whole claim-making surface — what
 * the three nodes assert about the pipeline — so it is tested as a pure function,
 * away from React Flow. The claims that matter are the ones that can be wrong in
 * a way that hides a fault.
 */

import { describe, it, expect } from "vitest"

import { buildLiveStreamStages, type LiveStreamConsumers } from "@/components/pipeline/LiveStreamGraph"
import type { PipelineRuntime, RuntimeDep } from "@/lib/hooks/usePipelineRuntime"

const dep = (kind: string, status: RuntimeDep["status"], last_error?: string): RuntimeDep => ({
  kind,
  identifier: `${kind}-1`,
  status,
  ...(last_error ? { last_error } : {}),
})

const runtime = (
  liveness: Partial<NonNullable<PipelineRuntime["liveness"]>> = {},
  dependencies: RuntimeDep[] = [],
): PipelineRuntime => ({
  pipeline_id: "p1",
  mode: "cdc",
  phase: "streaming",
  health: "healthy",
  dependencies,
  updated_at: new Date().toISOString(),
  liveness: { pending_events: 0, ...liveness },
})

const consumers = (topicCount: number): LiveStreamConsumers => ({
  measured: true,
  consumers: [
    {
      group: "rsync.sink-aa4c1a3c",
      role: "sink",
      total_lag: 0,
      topics: Array.from({ length: topicCount }, (_, i) => ({ topic: `t${i}`, lag: 0 })),
    },
  ],
})

const byId = (stages: ReturnType<typeof buildLiveStreamStages>) =>
  Object.fromEntries(stages.map((s) => [s.id, s]))

describe("buildLiveStreamStages", () => {
  it("is always source → kafka → destination, in that order", () => {
    const stages = buildLiveStreamStages(null, null, {})
    expect(stages.map((s) => s.id)).toEqual(["live_source", "live_kafka", "live_destination"])
    expect(stages[1].dependencies).toEqual(["live_source"])
    expect(stages[2].dependencies).toEqual(["live_kafka"])
  })

  it("names the real connectors when they are known", () => {
    const stages = buildLiveStreamStages(null, null, { source: "PostgreSQL", destination: "MongoDB" })
    expect(stages[0].display_name).toBe("Capture from PostgreSQL")
    expect(stages[2].display_name).toBe("Write to MongoDB")
  })

  // THE case this view exists for. With Debezium dead every topic drains to lag 0
  // and every other signal reads healthy (KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED),
  // so the source node is the one place the picture can be honest.
  it("fails the source node when capture has stopped, even with no backlog", () => {
    const stages = byId(
      buildLiveStreamStages(
        runtime({ sink_lag_messages: 0, sink_committed_moving: false }, [
          dep("debezium_task", "unhealthy", "debezium connector not found in kafka connect"),
        ]),
        null,
        {},
      ),
    )
    expect(stages.live_source.status).toBe("failed")
    expect(stages.live_source.result_summary).toContain("debezium connector not found")
    // And the downstream nodes must not contradict it by claiming completion.
    expect(stages.live_destination.status).not.toBe("complete")
  })

  it("marks the destination failed when the sink is stalled, not merely behind", () => {
    // A large backlog on a healthy sink is a first load. The Sentinel's own
    // two-signal verdict (backlog AND no committed progress) is what "stalled"
    // means, and only that turns this node red.
    const draining = byId(
      buildLiveStreamStages(
        runtime({ sink_lag_messages: 50_000, sink_committed_moving: true }, [dep("debezium_task", "healthy")]),
        null,
        {},
      ),
    )
    expect(draining.live_destination.status).toBe("running")
    expect(draining.live_kafka.status).toBe("running")
    expect(draining.live_kafka.result_summary).toBe("50,000 waiting")

    const stalled = byId(
      buildLiveStreamStages(
        runtime(
          { sink_lag_messages: 50_000, sink_committed_moving: false, sink_stalled: true, sink_stalled_seconds: 420 },
          [dep("debezium_task", "healthy")],
        ),
        null,
        {},
      ),
    )
    expect(stalled.live_destination.status).toBe("failed")
    expect(stalled.live_destination.result_summary).toContain("7m")
  })

  it("shows a healthy caught-up stream as up to date", () => {
    const stages = byId(
      buildLiveStreamStages(
        runtime({ sink_lag_messages: 0, sink_committed_moving: false }, [dep("debezium_task", "healthy")]),
        consumers(4),
        {},
      ),
    )
    expect(stages.live_source.status).toBe("running")
    expect(stages.live_kafka.status).toBe("complete")
    expect(stages.live_kafka.result_summary).toBe("nothing waiting")
    expect(stages.live_kafka.description).toBe("4 topics")
    expect(stages.live_destination.status).toBe("complete")
    expect(stages.live_destination.result_summary).toBe("up to date")
  })

  // Nothing measured must never render as a finished node: "complete" on this
  // graph is a claim that the work is done.
  it("leaves unmeasured nodes waiting, never complete", () => {
    const stages = byId(buildLiveStreamStages(null, null, {}))
    for (const id of ["live_source", "live_kafka", "live_destination"]) {
      expect(stages[id].status).toBe("waiting")
    }
    expect(stages.live_kafka.result_summary).toBe("no lag reading yet")

    // A runtime with no lag reading at all is the same answer.
    const noLag = byId(buildLiveStreamStages(runtime({}, [dep("debezium_task", "healthy")]), null, {}))
    expect(noLag.live_kafka.status).toBe("waiting")
  })

  it("degrades the topic label rather than the graph when the consumer read failed", () => {
    const stages = byId(
      buildLiveStreamStages(runtime({ sink_lag_messages: 0 }, [dep("debezium_task", "healthy")]), null, {}),
    )
    expect(stages.live_kafka.description).toBe("change stream")
    expect(stages.live_source.status).toBe("running")
  })

  it("reports a degraded source as still capturing, with the reason", () => {
    const stages = byId(
      buildLiveStreamStages(
        runtime({ sink_lag_messages: 0 }, [dep("debezium_task", "degraded", "one task is PAUSED")]),
        null,
        {},
      ),
    )
    expect(stages.live_source.status).toBe("running")
    expect(stages.live_source.result_summary).toBe("one task is PAUSED")
  })

  it("fails the destination when the destination connector is unreachable", () => {
    const stages = byId(
      buildLiveStreamStages(
        runtime({ sink_lag_messages: 0 }, [
          dep("debezium_task", "healthy"),
          dep("mcp_dest", "unhealthy", "no MCP server registered with orchestrator"),
        ]),
        null,
        {},
      ),
    )
    expect(stages.live_destination.status).toBe("failed")
    expect(stages.live_destination.result_summary).toContain("no MCP server registered")
  })
})
