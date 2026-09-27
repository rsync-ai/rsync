/**
 * The alerts panel carries three distinct classes of finding, and used to render
 * all of them in one shade of amber under the heading "Source Replication Lag".
 *
 * The Sentinel keys them by deliberately distinct ids so that neither lag resolver
 * can clear the other's alarm (`cdc_sentinel.go` sinkLagIssueID / connectorIssueID),
 * and they mean different things to whoever is on call:
 *
 *   cdc-lag-*             Debezium is behind the source's WAL/binlog
 *   cdc-sink-lag-*        changes are captured but are NOT reaching the destination
 *   cdc-connector-down-*  the connector is down and restarts have stopped working
 *
 * Two thirds of that was mislabelled, and the terminal connector-down escalation —
 * the only `critical` of the three — was painted the same colour as a transient
 * backlog.
 */

import { describe, it, expect } from "vitest"

import { alertClassLabel, alertIsCritical } from "@/components/pipeline/CDCLagAlertsPanel"

describe("alertClassLabel", () => {
  it("names each class from the id the Sentinel keys it by", () => {
    expect(alertClassLabel("cdc-lag-aa4c1a3c", "high_lag")).toBe("Source replication lag")
    expect(alertClassLabel("cdc-sink-lag-aa4c1a3c", "high_lag")).toBe("Not reaching the destination")
    expect(alertClassLabel("cdc-connector-down-aa4c1a3c", "connector_down")).toBe("Connector down")
  })

  it("does not confuse the two lag classes", () => {
    // `cdc-sink-lag-` also starts with `cdc-`, so a naive prefix check would file
    // a dead sink under "source replication lag" — the exact mislabel this fixes,
    // and the one the distinct ids exist to prevent on the backend.
    expect(alertClassLabel("cdc-sink-lag-x", "high_lag")).not.toBe("Source replication lag")
  })

  it("falls back to the readable type for a class it does not know", () => {
    // A future issue class must render as something a human can read rather than
    // vanish or show a raw token.
    expect(alertClassLabel("something-else", "schema_drift_blocked")).toBe("schema drift blocked")
  })
})

describe("alertIsCritical", () => {
  it("is true only for critical", () => {
    expect(alertIsCritical("critical")).toBe(true)
    expect(alertIsCritical("CRITICAL")).toBe(true)
    expect(alertIsCritical("warning")).toBe(false)
    expect(alertIsCritical("info")).toBe(false)
    expect(alertIsCritical("")).toBe(false)
  })
})
