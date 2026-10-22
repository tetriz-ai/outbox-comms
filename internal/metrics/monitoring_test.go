// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package metrics

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// TestMonitoringNamesExist pins that every outbox_comms_* series and label the
// alert rules and the Grafana dashboard query is one this package exports, so
// a rename here cannot leave a panel blank or an alert unable to fire.
func TestMonitoringNamesExist(t *testing.T) {
	labels := map[string]map[string]bool{}
	for _, c := range []prometheus.Collector{
		messageTransitionsTotal, dispatchDurationSeconds, deliveryLatencySeconds, deadTotal, unconfirmedTotal,
		orphanFeedbackTotal, feedbackDroppedTotal, schedulerEnqueueFailuresTotal, reconcileFailuresTotal, overduePending, bounceRate, complaintRate, reputationSampleSends,
	} {
		ch := make(chan *prometheus.Desc, 4)
		c.Describe(ch)
		close(ch)
		for d := range ch {
			s := d.String()
			name := regexp.MustCompile(`fqName: "([^"]+)"`).FindStringSubmatch(s)[1]
			set := map[string]bool{"job": true, "instance": true}
			if m := regexp.MustCompile(`variableLabels: \{([^}]*)\}`).FindStringSubmatch(s); m != nil {
				for _, l := range strings.Split(m[1], ",") {
					if l = strings.TrimSpace(l); l != "" {
						set[l] = true
					}
				}
			}
			labels[name] = set
			if strings.HasSuffix(name, "_seconds") {
				set["le"] = true
				for _, suffix := range []string{"_bucket", "_sum", "_count"} {
					labels[name+suffix] = set
				}
			}
		}
	}
	series := regexp.MustCompile(`(outbox_comms_[a-z_]+)(\{[^}]*\})?`)
	selector := regexp.MustCompile(`([a-z_]+)\s*(=~|!~|!=|=)`)
	grouping := regexp.MustCompile(`by ?\(([a-z_, ]+)\)`)
	checked := 0
	for _, file := range []string{"../../monitoring/alerts.yaml", "../../monitoring/alerts_test.yaml", "../../monitoring/grafana-dashboard.json"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ReplaceAll(string(raw), `\"`, `"`)
		for _, m := range series.FindAllStringSubmatch(text, -1) {
			checked++
			known, ok := labels[m[1]]
			if !ok {
				t.Errorf("%s: %s is not an exported metric", file, m[1])
				continue
			}
			for _, l := range selector.FindAllStringSubmatch(m[2], -1) {
				if !known[l[1]] {
					t.Errorf("%s: %s has no label %q", file, m[1], l[1])
				}
			}
		}
		for _, m := range grouping.FindAllStringSubmatch(text, -1) {
			for _, l := range strings.Split(m[1], ",") {
				l = strings.TrimSpace(l)
				found := false
				for _, set := range labels {
					found = found || set[l]
				}
				if !found {
					t.Errorf("%s: groups by %q, which no metric carries", file, l)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no outbox_comms_* series to check")
	}
}

// TestMonitoringLabelValuesExist pins the label values the alert rules and the
// dashboard match on: every to_status is a MessageStatus name and every
// reason_class is one of the Reason constants. A typo in either would leave a
// panel at zero or an alert that never fires.
func TestMonitoringLabelValuesExist(t *testing.T) {
	reasons := map[string]bool{ReasonTransientExhausted: true, ReasonPermanentConfig: true, ReasonRenderBug: true}
	known := map[string]func(string) bool{
		"to_status":    func(v string) bool { _, ok := pb.MessageStatus_value[v]; return ok },
		"reason_class": func(v string) bool { return reasons[v] },
	}
	matcher := regexp.MustCompile(`(to_status|reason_class)\s*(=~|!~|!=|=)\s*"([^"]*)"`)
	checked := 0
	for _, file := range []string{"../../monitoring/alerts.yaml", "../../monitoring/alerts_test.yaml", "../../monitoring/grafana-dashboard.json"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ReplaceAll(string(raw), `\"`, `"`)
		for _, m := range matcher.FindAllStringSubmatch(text, -1) {
			values := []string{m[3]}
			if strings.HasSuffix(m[2], "~") {
				values = strings.Split(m[3], "|")
			}
			for _, v := range values {
				checked++
				if !known[m[1]](v) {
					t.Errorf("%s: %s value %q is not one the service emits", file, m[1], v)
				}
			}
		}
	}
	if checked < 5 {
		t.Fatalf("checked %d label values; the matcher found too few to be reading the files", checked)
	}
}
