// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dispatch_test

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tetriz-ai/outbox-comms/internal/metrics"
)

// The tests read the metrics from the default registry, where the server
// registers them; importing the metrics package registers nothing.
func init() {
	if err := metrics.Register(prometheus.DefaultRegisterer); err != nil {
		panic(err)
	}
}
