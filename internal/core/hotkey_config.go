// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// HotKeyConfig is the scheduler-facing hot key tracking configuration.
// Default field values are defined in keylane.DefaultHotKeyConfig (public API).
type HotKeyConfig struct {
	Enabled                  bool
	MaxTrackedKeysPerShard   int
	DetectionWindow          time.Duration
	HotKeyDepthRatio         float64
	HotKeyWaitRatio          float64
	MaxCandidatesPerSnapshot int
	ExposeRawKey             bool
}
