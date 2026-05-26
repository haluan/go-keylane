// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

//go:build !race

package keylane

func submitRequestAllocSlack(jobAllocs float64) float64 {
	return jobAllocs + 6
}
