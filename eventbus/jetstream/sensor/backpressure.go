package sensor

import (
	"context"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/argoproj/argo-events/metrics"
)

// BackpressureWaiter checks ResourceQuota before allowing message fetch.
// This prevents fetching messages when downstream workflow capacity is exhausted,
// keeping messages safe in JetStream during backpressure conditions.
type BackpressureWaiter struct {
	kubeClient    kubernetes.Interface
	namespace     string
	quotaName     string
	resourceName  string        // e.g., "count/workflows.argoproj.io"
	capacityRatio float64       // e.g., 0.97 for 3% buffer
	pollInterval  time.Duration // How often to poll when blocked
	logger        *zap.SugaredLogger
	metrics       *metrics.Metrics
	sensorName    string
	triggerName   string
}

// BackpressureConfig holds configuration for BackpressureWaiter
type BackpressureConfig struct {
	QuotaName     string
	ResourceName  string        // Default: "count/workflows.argoproj.io"
	CapacityRatio float64       // Default: 0.97 (3% buffer)
	PollInterval  time.Duration // Default: 30s
	SensorName    string
	TriggerName   string
}

// NewBackpressureWaiter creates a new BackpressureWaiter
func NewBackpressureWaiter(
	kubeClient kubernetes.Interface,
	namespace string,
	config BackpressureConfig,
	m *metrics.Metrics,
	logger *zap.SugaredLogger,
) *BackpressureWaiter {
	// Apply defaults
	if config.ResourceName == "" {
		config.ResourceName = "count/workflows.argoproj.io"
	}
	if config.CapacityRatio <= 0 || config.CapacityRatio > 1 {
		config.CapacityRatio = 0.97 // 3% buffer by default
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 30 * time.Second
	}

	return &BackpressureWaiter{
		kubeClient:    kubeClient,
		namespace:     namespace,
		quotaName:     config.QuotaName,
		resourceName:  config.ResourceName,
		capacityRatio: config.CapacityRatio,
		pollInterval:  config.PollInterval,
		logger:        logger,
		metrics:       m,
		sensorName:    config.SensorName,
		triggerName:   config.TriggerName,
	}
}

// HasCapacity checks if there's capacity to process more workflows.
// Returns true if used < (hard * capacityRatio), false otherwise.
func (b *BackpressureWaiter) HasCapacity(ctx context.Context) (bool, error) {
	quota, err := b.kubeClient.CoreV1().ResourceQuotas(b.namespace).Get(ctx, b.quotaName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	resourceName := corev1.ResourceName(b.resourceName)
	hard := quota.Status.Hard[resourceName]
	used := quota.Status.Used[resourceName]

	hardVal := hard.Value()
	usedVal := used.Value()
	threshold := int64(float64(hardVal) * b.capacityRatio)

	hasCapacity := usedVal < threshold

	// TODO: Remove verbose logging after testing is complete
	// Verbose logging for testing - logs on every check
	b.logger.Infow("[BACKPRESSURE-TEST] Quota check performed",
		"quotaName", b.quotaName,
		"resourceName", b.resourceName,
		"hard", hardVal,
		"used", usedVal,
		"threshold", threshold,
		"hasCapacity", hasCapacity,
	)

	return hasCapacity, nil
}

// WaitForCapacity blocks until there's capacity available or context is cancelled.
// This should be called BEFORE fetching messages from JetStream.
func (b *BackpressureWaiter) WaitForCapacity(ctx context.Context) error {
	wasBlocked := false

	for {
		hasCapacity, err := b.HasCapacity(ctx)
		if err != nil {
			// Fail-closed: if we can't verify quota, don't fetch
			// This catches config errors (typos, RBAC issues) early
			if !wasBlocked {
				wasBlocked = true
				if b.metrics != nil {
					b.metrics.SetSensorQuotaBlocked(b.sensorName, b.triggerName, true)
				}
			}
			b.logger.Errorw("Failed to check quota, will retry (message stays safe in JetStream)",
				"error", err,
				"quotaName", b.quotaName,
				"pollInterval", b.pollInterval,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(b.pollInterval):
				continue // Retry checking quota
			}
		}

		if hasCapacity {
			// Clear blocked metric if we were blocked
			if wasBlocked {
				// TODO: Remove verbose logging after testing is complete
				b.logger.Infow("[BACKPRESSURE-TEST] Capacity available, resuming message fetch",
					"quotaName", b.quotaName,
					"wasBlocked", wasBlocked,
				)
				if b.metrics != nil {
					b.metrics.SetSensorQuotaBlocked(b.sensorName, b.triggerName, false)
				}
			} else {
				// TODO: Remove verbose logging after testing is complete
				b.logger.Infow("[BACKPRESSURE-TEST] Capacity available, proceeding with fetch",
					"quotaName", b.quotaName,
				)
			}
			return nil
		}

		// Mark as blocked on first iteration without capacity
		if !wasBlocked {
			wasBlocked = true
			// TODO: Remove verbose logging after testing is complete
			b.logger.Warnw("[BACKPRESSURE-TEST] No capacity, blocking message fetch",
				"quotaName", b.quotaName,
				"pollInterval", b.pollInterval,
			)
			if b.metrics != nil {
				b.metrics.SetSensorQuotaBlocked(b.sensorName, b.triggerName, true)
			}
		} else {
			// TODO: Remove verbose logging after testing is complete
			b.logger.Infow("[BACKPRESSURE-TEST] Still blocked, waiting for capacity",
				"quotaName", b.quotaName,
				"pollInterval", b.pollInterval,
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(b.pollInterval):
			// Continue checking
		}
	}
}

