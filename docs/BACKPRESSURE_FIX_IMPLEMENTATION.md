# Backpressure Fix Implementation - AIP-9946

## Overview

This document tracks the implementation of a pre-fetch quota check for Argo Events Sensors to prevent message loss during backpressure scenarios when NATS JetStream connections are lost.

**Jira Ticket:** AIP-9946  
**Author:** Abdul Rahman Abdurrab  
**Created:** 2026-01-06  
**Status:** In Progress

---

## Problem Statement

### The Bug: Orphaned ACK on NATS Connection Loss

When the downstream workflow ResourceQuota is full, the Sensor enters a backpressure state where it:
1. Fetches a message from JetStream
2. Attempts to trigger a workflow
3. Fails with "exceeded quota" error
4. Retries for up to 24 hours (via `resourceRetryStrategy`)
5. Sends `InProgress()` signals to JetStream to prevent redelivery

**The Problem:** If the NATS connection is lost during this retry period:
- The Sensor reconnects with a **new connection**
- The old connection's `InProgress()` and `AckSync()` calls fail
- The message becomes "orphaned" - the Sensor thinks it owns it, but JetStream may redeliver it or the Sensor gets stuck

### Root Cause

The Sensor fetches messages from JetStream **before** checking if downstream capacity is available. This means:
- Messages are held in Sensor memory during retries
- Messages are vulnerable to connection loss for extended periods (up to 24 hours)
- No way to return message to JetStream safely once fetched

### Symptoms Observed in Production

```
Error performing InProgress() on message
Error performing AckSync() on message
```

Sensor gets stuck and requires manual pod restart to recover.

---

## Solution: Pre-Fetch Quota Check

### Concept

**Before** fetching a message from JetStream, check if the downstream ResourceQuota has capacity. If not, **wait** until capacity is available. This keeps messages safe in JetStream's durable storage during backpressure.

### Key Benefits

| Aspect | Before (Current) | After (With Fix) |
|--------|------------------|------------------|
| Message location during backpressure | Sensor memory | JetStream (durable) |
| Vulnerability to connection loss | High (hours) | Low (milliseconds) |
| Recovery after pod restart | Manual intervention | Automatic (JetStream redelivers) |
| API calls during backpressure | 2 NATS calls/sec (`InProgress()`) | 1 K8s call/30sec (quota check) |

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         SENSOR POD                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  BackpressureWaiter.WaitForCapacity(ctx)                    ││
│  │    ├── Check ResourceQuota: used < (hard * 0.97)            ││
│  │    ├── If capacity: return (allow fetch)                    ││
│  │    └── If no capacity: sleep 30s, retry                     ││
│  └─────────────────────────────────────────────────────────────┘│
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  subscription.Fetch(1)  ← Only called when capacity exists  ││
│  └─────────────────────────────────────────────────────────────┘│
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Trigger Workflow → msg.AckSync()                           ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Details

### Files Changed

| File | Purpose |
|------|---------|
| `eventbus/jetstream/sensor/backpressure.go` | **NEW** - BackpressureWaiter implementation |
| `eventbus/jetstream/sensor/trigger_conn.go` | Add backpressure check before `subscription.Fetch()` |
| `sensors/listener.go` | Initialize BackpressureWaiter with config from env vars |
| `metrics/metrics.go` | Add `sensor_quota_blocked` gauge metric |
| `common/common.go` | Add constants for env var names and defaults |

### Configuration (Environment Variables)

| Env Var | Required | Default | Description |
|---------|----------|---------|-------------|
| `BACKPRESSURE_QUOTA_NAME` | Yes | - | Name of the ResourceQuota to check |
| `BACKPRESSURE_RESOURCE_NAME` | No | `count/workflows.argoproj.io` | Resource to check in quota |
| `BACKPRESSURE_CAPACITY_RATIO` | No | `0.97` | Capacity threshold (3% buffer) |
| `BACKPRESSURE_POLL_INTERVAL` | No | `30s` | How often to poll when blocked |

### Example Sensor Deployment Patch

```yaml
spec:
  template:
    spec:
      containers:
        - name: main
          env:
            - name: BACKPRESSURE_QUOTA_NAME
              value: "workflow-concurrency-test"
            - name: BACKPRESSURE_CAPACITY_RATIO
              value: "0.97"
```

### Metrics Added

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `sensor_quota_blocked` | Gauge | sensor_name, trigger_name | 1 when blocked, 0 when not |

---

## RBAC Requirements

The Sensor's ServiceAccount needs `get` permission on `resourcequotas`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sensor-quota-reader
rules:
  - apiGroups: [""]
    resources: ["resourcequotas"]
    verbs: ["get"]
```

**Verified:** The `default-editor` ServiceAccount (used by Sensors in Kubeflow namespaces) already has this permission via the `kubeflow-kubernetes-edit` ClusterRole.

---

## Testing

### Test Environment

- **Cluster:** `aianalytics-sandbox-k8s-1`
- **Namespace:** `zap-aip-synthetic-eventing-sqs-test-sandbox`
- **Test Branch:** `abdula-backpressure-test` (in `aip-synthetic-eventing-sqs-test` repo)

### Test Configuration

| Setting | Value |
|---------|-------|
| EventBus `maxMsgs` | 5 |
| ResourceQuota `count/workflows.argoproj.io` | 5 |
| Workflow sleep time | 60-240 seconds (random) |
| `resourceRetryStrategy` | 1440 steps × 60s (24 hours) |

### Reproduction Steps

1. Deploy test infrastructure from `abdula-backpressure-test` branch
2. Send 8 SQS messages via trigger workflow
3. 5 workflows start, 3 messages get quota-blocked
4. Sensor retries with `resourceRetryStrategy` (60s intervals)
5. Restart EventBus pods to simulate NATS connection drop
6. Check for orphaned ACK errors in Sensor logs

### Test Results (2026-01-06)

| Step | Result |
|------|--------|
| Quota saturation (5/5) | ✅ Verified |
| Sensor retry on quota errors | ✅ Working (`DoWithResourceAwareRetry`) |
| EventBus connection drop + reconnect | ✅ Sensor reconnected |
| Messages processed after quota freed | ✅ 3 remaining messages processed |
| Orphaned ACK bug manifested | ⚠️ Did not reproduce (timing-dependent) |

**Note:** The bug is timing-dependent - requires connection loss during active `InProgress()` call while message is in-flight.

---

## Implementation Checklist

### Completed ✅

- [x] Identify root cause of orphaned ACK bug
- [x] Design pre-fetch quota check solution
- [x] Verify RBAC requirements (no changes needed)
- [x] Create `backpressure.go` with `BackpressureWaiter` implementation
- [x] Add `sensor_quota_blocked` metric to `metrics.go`
- [x] Add constants to `common/common.go`
- [x] Set up test environment with `maxMsgs: 5` and quota limit
- [x] Verify quota saturation and retry behavior
- [x] Document implementation approach

### In Progress 🔄

- [x] Integrate `BackpressureWaiter` into `trigger_conn.go` ✅ (lines 260-267)
- [x] Update `listener.go` to initialize backpressure from env vars ✅ (lines 179-196)
- [ ] Commit and push to GitLab (`abdula/AIP-9985-backpressure-prefetch` branch)
- [ ] Build new Sensor image with fix (CI/CD)
- [ ] Deploy and test with backpressure scenario

### Pending ⏳

- [ ] Verify fix prevents stuck Sensor on connection loss (no quota errors expected)
- [ ] Performance testing (API call overhead)
- [ ] Update runbooks for new metric alerting
- [ ] Create MR for `argo-events` repo
- [ ] Merge to `feature/zg` branch
- [ ] Deploy to stage environment
- [ ] Deploy to production

---

## Key Code Snippets

### BackpressureWaiter Core Logic

```go
// WaitForCapacity blocks until there's capacity available or context is cancelled.
func (b *BackpressureWaiter) WaitForCapacity(ctx context.Context) error {
    wasBlocked := false
    for {
        hasCapacity, err := b.HasCapacity(ctx)
        if err != nil {
            // Fail-closed: if we can't verify quota, don't fetch
            b.logger.Errorw("Failed to check quota, will retry", "error", err)
            b.metrics.SetSensorQuotaBlocked(b.sensorName, b.triggerName, true)
            wasBlocked = true
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(b.pollInterval):
                continue
            }
        }

        if hasCapacity {
            if wasBlocked && b.metrics != nil {
                b.metrics.SetSensorQuotaBlocked(b.sensorName, b.triggerName, false)
            }
            return nil
        }

        // Block until capacity available
        if !wasBlocked {
            b.logger.Infow("Quota near capacity, will wait before fetching",
                "quotaName", b.quotaName)
            b.metrics.SetSensorQuotaBlocked(b.sensorName, b.triggerName, true)
            wasBlocked = true
        }

        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(b.pollInterval):
            // Continue checking
        }
    }
}
```

### Integration Point in trigger_conn.go

```go
func (conn *JetstreamTriggerConn) pullSubscribe(...) {
    for {
        // ✅ PRE-FETCH QUOTA CHECK
        if conn.backpressureWaiter != nil {
            if err := conn.backpressureWaiter.WaitForCapacity(ctx); err != nil {
                conn.Logger.Warnw("Backpressure wait cancelled", "error", err)
                return
            }
        }

        // Only fetch when we have capacity
        msgs, fetchErr := subscription.Fetch(1, nats.MaxWait(time.Second*1))
        // ...
    }
}
```

---

## Related Resources

### Repositories

| Repo | Purpose | Branch |
|------|---------|--------|
| `argo-events` (GitLab) | Sensor/EventSource code | `abdula/AIP-9985-backpressure-prefetch` (based on `feature/zg` v1.9.2) |
| `aip-synthetic-eventing-sqs-test` (GitLab) | Test infrastructure | `abdula-backpressure-test` |

### Image Versions

| Component | Current Image | Base Version |
|-----------|---------------|--------------|
| Controller | `argo-events:v1.8.0-0.1.74` | v1.8.0 (`feature/zg-1.8`) |
| EventSource/Sensor | `argo-events:v1.9.2-0.1.70` | v1.9.2 (`feature/zg`) |

### Key Files in argo-events

- `eventbus/jetstream/sensor/trigger_conn.go` - JetStream connection handling
- `eventbus/jetstream/sensor/backpressure.go` - Backpressure implementation
- `sensors/listener.go` - Sensor main loop
- `common/retry.go` - `DoWithResourceAwareRetry` implementation
- `metrics/metrics.go` - Prometheus metrics

### Commands for Testing

```bash
# Switch to sandbox context
aws --profile aianalytics-sandbox-k8s-kubeflow-admin --region us-west-2 \
    eks update-kubeconfig --name aianalytics-sandbox-k8s-1 --alias aianalytics-sandbox-k8s-1
kubectl config use-context aianalytics-sandbox-k8s-1

# Check quota status
kubectl get resourcequota workflow-concurrency-test \
    -n zap-aip-synthetic-eventing-sqs-test-sandbox \
    -o jsonpath='hard: {.status.hard}  used: {.status.used}'

# Check sensor logs for quota errors
kubectl logs -n zap-aip-synthetic-eventing-sqs-test-sandbox \
    -l sensor-name=aip-synthetic-eventing-sqs-test --tail=50 \
    | grep -E "(exceeded quota|AckSync|InProgress)"

# Send test messages (trigger workflow)
kubectl create -n zap-aip-synthetic-tests-sandbox -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: test-trigger-
spec:
  workflowTemplateRef:
    name: eventing-sqs-trigger-aip-9738-v7-test-2a8d66db
EOF
```

---

## Contact

- **Owner:** Abdul Rahman Abdurrab (@abdula)
- **Team:** AI Platform Infrastructure
- **Slack:** #ai-platform-oncall

---

*Last Updated: 2026-01-06*

