/*
Copyright 2018 BlackRock, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package common

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/argoproj/argo-events/pkg/apis/sensor/v1alpha1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"

	apicommon "github.com/argoproj/argo-events/pkg/apis/common"
)

func TestRetryableKubeAPIError(t *testing.T) {
	errUnAuth := errors.NewUnauthorized("reason")
	errNotFound := errors.NewNotFound(v1alpha1.Resource("sensor"), "hello")
	errForbidden := errors.NewForbidden(v1alpha1.Resource("sensor"), "hello", nil)
	errInvalid := errors.NewInvalid(v1alpha1.Kind("core/data"), "hello", nil)
	errMethodNotSupported := errors.NewMethodNotSupported(v1alpha1.Resource("sensor"), "action")

	assert.True(t, IsRetryableKubeAPIError(errUnAuth))
	assert.False(t, IsRetryableKubeAPIError(errNotFound))
	assert.False(t, IsRetryableKubeAPIError(errForbidden))
	assert.False(t, IsRetryableKubeAPIError(errInvalid))
	assert.False(t, IsRetryableKubeAPIError(errMethodNotSupported))
}

func TestConnect(t *testing.T) {
	err := DoWithRetry(nil, func() error {
		return fmt.Errorf("new error")
	})
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "new error"))

	err = DoWithRetry(nil, func() error {
		return nil
	})
	assert.Nil(t, err)
}

func TestConnectDurationString(t *testing.T) {
	start := time.Now()
	count := 2
	err := DoWithRetry(nil, func() error {
		if count == 0 {
			return nil
		} else {
			count--
			return fmt.Errorf("new error")
		}
	})
	end := time.Now()
	elapsed := end.Sub(start)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.True(t, elapsed >= 2*time.Second)
}

func TestConnectRetry(t *testing.T) {
	factor := apicommon.NewAmount("1.0")
	jitter := apicommon.NewAmount("1")
	duration := apicommon.FromInt64(1000000000)
	backoff := apicommon.Backoff{
		Duration: &duration,
		Factor:   &factor,
		Jitter:   &jitter,
		Steps:    5,
	}
	count := 2
	start := time.Now()
	err := DoWithRetry(&backoff, func() error {
		if count == 0 {
			return nil
		} else {
			count--
			return fmt.Errorf("new error")
		}
	})
	end := time.Now()
	elapsed := end.Sub(start)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.True(t, elapsed >= 2*time.Second)
}

func TestRetryFailure(t *testing.T) {
	factor := apicommon.NewAmount("1.0")
	jitter := apicommon.NewAmount("1")
	duration := apicommon.FromString("1s")
	backoff := apicommon.Backoff{
		Duration: &duration,
		Factor:   &factor,
		Jitter:   &jitter,
		Steps:    2,
	}
	err := DoWithRetry(&backoff, func() error {
		return fmt.Errorf("this is an error")
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "after retries")
	assert.Contains(t, err.Error(), "this is an error")
}

func TestConvert2WaitBackoff(t *testing.T) {
	factor := apicommon.NewAmount("1.0")
	jitter := apicommon.NewAmount("1")
	duration := apicommon.FromString("1s")
	backoff := apicommon.Backoff{
		Duration: &duration,
		Factor:   &factor,
		Jitter:   &jitter,
		Steps:    2,
	}
	waitBackoff, err := Convert2WaitBackoff(&backoff)
	assert.NoError(t, err)
	assert.Equal(t, wait.Backoff{
		Duration: 1 * time.Second,
		Factor:   1.0,
		Jitter:   1.0,
		Steps:    2,
	}, *waitBackoff)
}

func TestIsResourceConstraintError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "quota exceeded forbidden error",
			err:      errors.NewForbidden(v1alpha1.Resource("workflows"), "test", fmt.Errorf("exceeded quota: workflow-limit")),
			expected: true,
		},
		{
			name:     "regular forbidden error",
			err:      errors.NewForbidden(v1alpha1.Resource("sensor"), "test", fmt.Errorf("access denied")),
			expected: false,
		},
		{
			name:     "not found error",
			err:      errors.NewNotFound(v1alpha1.Resource("sensor"), "test"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsResourceConstraintError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDoWithResourceAwareRetry(t *testing.T) {
	t.Run("successful execution", func(t *testing.T) {
		callCount := 0
		err := DoWithResourceAwareRetry(nil, nil, func() error {
			callCount++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, callCount)
	})

	t.Run("regular error uses default backoff", func(t *testing.T) {
		callCount := 0
		defaultBackoff := &apicommon.Backoff{Steps: 2}
		err := DoWithResourceAwareRetry(defaultBackoff, nil, func() error {
			callCount++
			if callCount < 2 {
				return fmt.Errorf("regular error")
			}
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, callCount)
	})

	t.Run("resource constraint error uses resource backoff", func(t *testing.T) {
		callCount := 0
		defaultBackoff := &apicommon.Backoff{Steps: 5}
		resourceBackoff := &apicommon.Backoff{Steps: 2}
		
		err := DoWithResourceAwareRetry(defaultBackoff, resourceBackoff, func() error {
			callCount++
			if callCount < 2 {
				return errors.NewForbidden(v1alpha1.Resource("pods"), "test", fmt.Errorf("exceeded quota"))
			}
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, callCount)
	})

	t.Run("resource constraint error without resource backoff uses default", func(t *testing.T) {
		callCount := 0
		defaultBackoff := &apicommon.Backoff{Steps: 2}
		
		err := DoWithResourceAwareRetry(defaultBackoff, nil, func() error {
			callCount++
			if callCount < 2 {
				return errors.NewForbidden(v1alpha1.Resource("pods"), "test", fmt.Errorf("exceeded quota"))
			}
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, callCount)
	})

	t.Run("all retries exhausted", func(t *testing.T) {
		callCount := 0
		defaultBackoff := &apicommon.Backoff{Steps: 2}
		
		err := DoWithResourceAwareRetry(defaultBackoff, nil, func() error {
			callCount++
			return fmt.Errorf("persistent error")
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed after retries")
		assert.Contains(t, err.Error(), "persistent error")
		assert.Equal(t, 3, callCount) // Initial attempt + 2 retries
	})
}
