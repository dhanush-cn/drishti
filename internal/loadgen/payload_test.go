package loadgen

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestLogPool_ShapeIsExactAndVaried(t *testing.T) {
	shape := LogShape{BatchSize: 512, Services: 20, BodyMin: 80, BodyMax: 400, AttrsMin: 5, AttrsMax: 8}
	pool, err := LogPool(shape, 64, "run-x", 7)
	if err != nil {
		t.Fatal(err)
	}
	perService := map[string]int{}
	bodies := map[string]bool{}
	for j, req := range pool {
		if n := CountLogRecords(req); n != 512 {
			t.Fatalf("request %d has %d records, want 512", j, n)
		}
		if len(req.ResourceLogs) != shape.ResourcesPerRequest() {
			t.Fatalf("request %d has %d resources, want %d", j, len(req.ResourceLogs), shape.ResourcesPerRequest())
		}
		for _, rl := range req.ResourceLogs {
			var svc, run string
			for _, kv := range rl.Resource.Attributes {
				switch kv.Key {
				case "service.name":
					svc = kv.Value.GetStringValue()
				case RunIDAttr:
					run = kv.Value.GetStringValue()
				}
			}
			if run != "run-x" {
				t.Fatalf("resource without %s", RunIDAttr)
			}
			perService[svc]++
			for _, r := range rl.ScopeLogs[0].LogRecords {
				b := r.Body.GetStringValue()
				if len(b) < 80 || len(b) > 400 {
					t.Fatalf("body length %d outside 80..400", len(b))
				}
				if n := len(r.Attributes); n < 5 || n > 8 {
					t.Fatalf("%d attributes outside 5..8", n)
				}
				bodies[b] = true
			}
		}
	}
	if len(perService) != 20 {
		t.Fatalf("%d distinct services, want 20", len(perService))
	}
	for s, n := range perService {
		if n != perService["svc-0"] {
			t.Fatalf("service %s appears %d times, svc-0 %d: keys unevenly spread", s, n, perService["svc-0"])
		}
	}
	// Distinct payloads, not one repeated: compressors must not see the
	// same bytes every send (§6).
	if len(bodies) < 64*512/2 {
		t.Fatalf("only %d distinct bodies in %d records", len(bodies), 64*512)
	}
	if proto.Equal(pool[0], pool[1]) {
		t.Fatal("pool requests 0 and 1 are identical")
	}
}

func TestLogPool_HotPartitionMode(t *testing.T) {
	pool, err := LogPool(LogShape{BatchSize: 100, Services: 1, BodyMin: 80, BodyMax: 80, AttrsMin: 0, AttrsMax: 0}, 4, "r", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range pool {
		if len(req.ResourceLogs) != 1 || CountLogRecords(req) != 100 {
			t.Fatalf("-services 1: %d resources, %d records", len(req.ResourceLogs), CountLogRecords(req))
		}
	}
}

func TestLogPool_SameSeedIsReproducible(t *testing.T) {
	shape := LogShape{BatchSize: 20, Services: 3, BodyMin: 80, BodyMax: 120, AttrsMin: 1, AttrsMax: 3}
	a, _ := LogPool(shape, 4, "r", 42)
	b, _ := LogPool(shape, 4, "r", 42)
	for i := range a {
		for j := range a[i].ResourceLogs {
			ra, rb := a[i].ResourceLogs[j].ScopeLogs[0].LogRecords, b[i].ResourceLogs[j].ScopeLogs[0].LogRecords
			for k := range ra {
				if ra[k].Body.GetStringValue() != rb[k].Body.GetStringValue() {
					t.Fatal("same seed produced different bodies")
				}
			}
		}
	}
}

func TestLogShape_Validate(t *testing.T) {
	bad := []LogShape{
		{BatchSize: 0, Services: 1, BodyMin: 1, BodyMax: 1},
		{BatchSize: 1, Services: 0, BodyMin: 1, BodyMax: 1},
		{BatchSize: 1, Services: 1, BodyMin: 10, BodyMax: 5},
		{BatchSize: 1, Services: 1, BodyMin: 1, BodyMax: 1, AttrsMin: 0, AttrsMax: 99},
	}
	for i, s := range bad {
		if s.Validate() == nil {
			t.Errorf("shape %d should be invalid: %+v", i, s)
		}
	}
}
