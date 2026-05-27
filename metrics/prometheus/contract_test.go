// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package prometheus

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/haluan/go-keylane"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricNamesFollowContract(t *testing.T) {
	q := v05MetricsTestQueue(t)
	warmPerKeyDecisionMetrics(t, q)
	c := NewCollector(q, CollectorOptions{SchedulerName: "contract"})
	reg := prom.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatal(err)
	}
	_ = q.Submit(context.Background(), keylane.Job{
		Key: "contract-key", Lane: "default",
		Run: func(context.Context) error { return nil },
	})

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	contractByName := make(map[string]keylane.MetricDescriptor, len(keylane.StableMetricDescriptors()))
	for _, d := range keylane.StableMetricDescriptors() {
		contractByName[d.Name] = d
	}

	for _, mf := range mfs {
		name := mf.GetName()
		desc, ok := contractByName[name]
		if !ok {
			t.Errorf("scraped metric %q not in keylane.StableMetricDescriptors()", name)
			continue
		}
		wantLabels := labelNamesFromDescriptor(desc)
		gotLabels := metricFamilyLabelNames(mf)
		if !sameStringSet(wantLabels, gotLabels) {
			t.Errorf("metric %q: label names = %v, want %v", name, gotLabels, wantLabels)
		}
		delete(contractByName, name)
	}

	// Core families should appear even on a quiet queue; remaining entries are optional at scrape time.
	for name := range contractByName {
		if strings.Contains(name, "hot_key") || strings.Contains(name, "per_key") || strings.Contains(name, "scale_") {
			continue
		}
		t.Errorf("stable contract metric %q was not scraped", name)
	}
}

func TestStableMetricDescriptorsMatchPrometheusInventory(t *testing.T) {
	want := len(keylane.StableMetricDescriptors())
	got := len(allDescriptors())
	if want != got {
		t.Fatalf("StableMetricDescriptors count = %d, prometheus allDescriptors = %d", want, got)
	}
}

func labelNamesFromDescriptor(d keylane.MetricDescriptor) []string {
	names := make([]string, len(d.Labels))
	for i, l := range d.Labels {
		names[i] = l.Name
	}
	sort.Strings(names)
	return names
}

func metricFamilyLabelNames(mf *dto.MetricFamily) []string {
	if mf == nil || len(mf.GetMetric()) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			seen[lp.GetName()] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
