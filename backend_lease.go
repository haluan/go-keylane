// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "sync"

// BackendLease grants permission to use a backend resource lane until Release is called.
type BackendLease interface {
	Release()
}

type noopBackendLease struct{}

func (noopBackendLease) Release() {}

type trackedBackendLease struct {
	once    sync.Once
	release func()
}

func (l *trackedBackendLease) Release() {
	l.once.Do(func() {
		if l.release != nil {
			l.release()
		}
	})
}
