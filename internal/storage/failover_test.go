package storage

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNew_GroupMembershipOptions(t *testing.T) {
	e := newEnv(t, 1)
	c := e.consumerWith(fast, func(cfg *Config) {
		cfg.InstanceID = "storage-1"
		cfg.SessionTimeout = 6 * time.Second
	})
	defer c.cl.Close()
	if id, _ := c.cl.OptValue(kgo.InstanceID).(string); id != "storage-1" {
		t.Errorf("InstanceID = %v, want storage-1", c.cl.OptValue(kgo.InstanceID))
	}
	if got := c.cl.OptValue(kgo.SessionTimeout); got != 6*time.Second {
		t.Errorf("SessionTimeout = %v, want 6s", got)
	}
	if got := c.cl.OptValue(kgo.HeartbeatInterval); got != 2*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 2s (a third of the session)", got)
	}
	d := e.consumerWith(fast, nil)
	defer d.cl.Close()
	if id, _ := d.cl.OptValue(kgo.InstanceID).(string); id != "" {
		t.Errorf("no InstanceID configured, client has %q", id)
	}
}

// A static member that stops does not leave the group (franz-go sends no
// LeaveGroup for static members), which is what a kill -9 looks like to the
// group. Its replacement with the same instance ID takes over at once; a
// replacement under a new ID has to wait for the old session to expire.
func TestRun_StaticMemberRestartTakesOverAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name       string
		restartID  string
		wantWithin time.Duration
	}{
		{"same instance ID", "storage-1", 6 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, 2)
			const session = 25 * time.Second
			edit := func(id string) func(*Config) {
				return func(cfg *Config) { cfg.InstanceID = id; cfg.SessionTimeout = session }
			}
			want := e.produce(0, "p1", 3, 4, time.Now()) + e.produce(1, "p1", 3, 4, time.Now())
			if err := e.runUntil(e.consumerWith(fast, edit("storage-1")), want, 10*time.Second); err != nil {
				t.Fatal(err)
			}
			want += e.produce(0, "p2", 3, 4, time.Now()) + e.produce(1, "p2", 3, 4, time.Now())
			start := time.Now()
			c := e.consumerWith(fast, edit(tc.restartID))
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx) }()
			defer func() { cancel(); <-done }()
			waitUntil(t, tc.wantWithin, "restarted member stores the new records", func() bool { return len(e.ch.snapshot()) == want })
			t.Logf("takeover + store: %v (session timeout %v)", time.Since(start).Round(100*time.Millisecond), session)
			if d := dups(e.ch.snapshot()); len(d) > 0 {
				t.Fatalf("duplicates: %v", d)
			}
		})
	}
}
