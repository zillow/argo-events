package sensor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewBackpressureWaiter(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	logger := zap.NewNop().Sugar()

	t.Run("applies defaults for empty config", func(t *testing.T) {
		config := BackpressureConfig{
			QuotaName: "test-quota",
		}
		waiter := NewBackpressureWaiter(kubeClient, "default", config, nil, logger)

		assert.Equal(t, "test-quota", waiter.quotaName)
		assert.Equal(t, "count/workflows.argoproj.io", waiter.resourceName)
		assert.Equal(t, 0.95, waiter.capacityRatio)
		assert.Equal(t, 30*time.Second, waiter.pollInterval)
	})

	t.Run("uses provided config values", func(t *testing.T) {
		config := BackpressureConfig{
			QuotaName:     "custom-quota",
			ResourceName:  "count/pods",
			CapacityRatio: 0.8,
			PollInterval:  10 * time.Second,
		}
		waiter := NewBackpressureWaiter(kubeClient, "test-ns", config, nil, logger)

		assert.Equal(t, "custom-quota", waiter.quotaName)
		assert.Equal(t, "count/pods", waiter.resourceName)
		assert.Equal(t, 0.8, waiter.capacityRatio)
		assert.Equal(t, 10*time.Second, waiter.pollInterval)
	})

	t.Run("corrects invalid capacity ratio", func(t *testing.T) {
		config := BackpressureConfig{
			QuotaName:     "test-quota",
			CapacityRatio: 1.5, // Invalid - greater than 1
		}
		waiter := NewBackpressureWaiter(kubeClient, "default", config, nil, logger)
		assert.Equal(t, 0.95, waiter.capacityRatio)

		config.CapacityRatio = -0.5 // Invalid - negative
		waiter = NewBackpressureWaiter(kubeClient, "default", config, nil, logger)
		assert.Equal(t, 0.95, waiter.capacityRatio)

		config.CapacityRatio = 0 // Invalid - zero
		waiter = NewBackpressureWaiter(kubeClient, "default", config, nil, logger)
		assert.Equal(t, 0.95, waiter.capacityRatio)
	})
}

func TestHasCapacity(t *testing.T) {
	logger := zap.NewNop().Sugar()
	ctx := context.Background()

	t.Run("returns true when under threshold", func(t *testing.T) {
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-quota",
				Namespace: "default",
			},
			Status: corev1.ResourceQuotaStatus{
				Hard: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("100"),
				},
				Used: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("90"), // 90% used, threshold is 95%
				},
			},
		}
		kubeClient := fake.NewSimpleClientset(quota)

		waiter := NewBackpressureWaiter(kubeClient, "default", BackpressureConfig{
			QuotaName:     "test-quota",
			CapacityRatio: 0.95,
		}, nil, logger)

		hasCapacity, err := waiter.HasCapacity(ctx)
		assert.NoError(t, err)
		assert.True(t, hasCapacity)
	})

	t.Run("returns false when at threshold", func(t *testing.T) {
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-quota",
				Namespace: "default",
			},
			Status: corev1.ResourceQuotaStatus{
				Hard: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("100"),
				},
				Used: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("95"), // At threshold
				},
			},
		}
		kubeClient := fake.NewSimpleClientset(quota)

		waiter := NewBackpressureWaiter(kubeClient, "default", BackpressureConfig{
			QuotaName:     "test-quota",
			CapacityRatio: 0.95,
		}, nil, logger)

		hasCapacity, err := waiter.HasCapacity(ctx)
		assert.NoError(t, err)
		assert.False(t, hasCapacity)
	})

	t.Run("returns false when over threshold", func(t *testing.T) {
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-quota",
				Namespace: "default",
			},
			Status: corev1.ResourceQuotaStatus{
				Hard: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("100"),
				},
				Used: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("98"),
				},
			},
		}
		kubeClient := fake.NewSimpleClientset(quota)

		waiter := NewBackpressureWaiter(kubeClient, "default", BackpressureConfig{
			QuotaName:     "test-quota",
			CapacityRatio: 0.95,
		}, nil, logger)

		hasCapacity, err := waiter.HasCapacity(ctx)
		assert.NoError(t, err)
		assert.False(t, hasCapacity)
	})

	t.Run("returns error when quota not found", func(t *testing.T) {
		kubeClient := fake.NewSimpleClientset() // No quota

		waiter := NewBackpressureWaiter(kubeClient, "default", BackpressureConfig{
			QuotaName: "nonexistent-quota",
		}, nil, logger)

		hasCapacity, err := waiter.HasCapacity(ctx)
		assert.Error(t, err)
		assert.False(t, hasCapacity)
	})
}

func TestWaitForCapacity(t *testing.T) {
	logger := zap.NewNop().Sugar()

	t.Run("returns immediately when capacity available", func(t *testing.T) {
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-quota",
				Namespace: "default",
			},
			Status: corev1.ResourceQuotaStatus{
				Hard: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("100"),
				},
				Used: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("50"),
				},
			},
		}
		kubeClient := fake.NewSimpleClientset(quota)

		waiter := NewBackpressureWaiter(kubeClient, "default", BackpressureConfig{
			QuotaName:    "test-quota",
			PollInterval: 100 * time.Millisecond,
		}, nil, logger)

		ctx := context.Background()
		start := time.Now()
		err := waiter.WaitForCapacity(ctx)
		elapsed := time.Since(start)

		assert.NoError(t, err)
		assert.Less(t, elapsed, 50*time.Millisecond) // Should return almost immediately
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-quota",
				Namespace: "default",
			},
			Status: corev1.ResourceQuotaStatus{
				Hard: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("100"),
				},
				Used: corev1.ResourceList{
					"count/workflows.argoproj.io": resource.MustParse("100"), // Full
				},
			},
		}
		kubeClient := fake.NewSimpleClientset(quota)

		waiter := NewBackpressureWaiter(kubeClient, "default", BackpressureConfig{
			QuotaName:    "test-quota",
			PollInterval: 1 * time.Second,
		}, nil, logger)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := waiter.WaitForCapacity(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
	})
}

func TestThresholdCalculation(t *testing.T) {
	logger := zap.NewNop().Sugar()
	ctx := context.Background()

	tests := []struct {
		name          string
		hard          int64
		used          int64
		capacityRatio float64
		expectCapacity bool
	}{
		{
			name:          "150 quota, 0.95 ratio, 140 used = has capacity",
			hard:          150,
			used:          140,
			capacityRatio: 0.95,
			expectCapacity: true, // threshold = 142, used < threshold
		},
		{
			name:          "150 quota, 0.95 ratio, 143 used = no capacity",
			hard:          150,
			used:          143,
			capacityRatio: 0.95,
			expectCapacity: false, // threshold = 142, used >= threshold
		},
		{
			name:          "5 quota, 0.95 ratio, 4 used = no capacity",
			hard:          5,
			used:          4,
			capacityRatio: 0.95,
			expectCapacity: false, // threshold = 4, used >= threshold
		},
		{
			name:          "5 quota, 0.95 ratio, 3 used = has capacity",
			hard:          5,
			used:          3,
			capacityRatio: 0.95,
			expectCapacity: true, // threshold = 4, used < threshold
		},
		{
			name:          "100 quota, 0.80 ratio, 79 used = has capacity",
			hard:          100,
			used:          79,
			capacityRatio: 0.80,
			expectCapacity: true, // threshold = 80, used < threshold
		},
		{
			name:          "100 quota, 0.80 ratio, 80 used = no capacity",
			hard:          100,
			used:          80,
			capacityRatio: 0.80,
			expectCapacity: false, // threshold = 80, used >= threshold
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quota := &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-quota",
					Namespace: "default",
				},
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{
						"count/workflows.argoproj.io": *resource.NewQuantity(tt.hard, resource.DecimalSI),
					},
					Used: corev1.ResourceList{
						"count/workflows.argoproj.io": *resource.NewQuantity(tt.used, resource.DecimalSI),
					},
				},
			}
			kubeClient := fake.NewSimpleClientset(quota)

			waiter := NewBackpressureWaiter(kubeClient, "default", BackpressureConfig{
				QuotaName:     "test-quota",
				CapacityRatio: tt.capacityRatio,
			}, nil, logger)

			hasCapacity, err := waiter.HasCapacity(ctx)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectCapacity, hasCapacity)
		})
	}
}

