package keylane

import "testing"

func TestShutdownOptionsDefault(t *testing.T) {
	cfg := stopConfig{
		drain: true,
	}

	opt := WithDrain(false)
	opt(&cfg)

	if cfg.drain {
		t.Errorf("expected drain to be false, got true")
	}
}
