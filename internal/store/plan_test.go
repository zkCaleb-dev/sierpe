package store

import (
	"fmt"
	"testing"
)

func TestNormalizePlanPadsAlignsAndMerges(t *testing.T) {
	cases := []struct {
		name    string
		in      []Interval
		padding uint32
		want    []Interval
	}{
		{
			name: "snaps to the enclosing checkpoint",
			in:   []Interval{{From: 1000, To: 1000}},
			want: []Interval{{From: 960, To: 1023}},
		},
		{
			name:    "padding widens before the snap",
			in:      []Interval{{From: 1000, To: 1000}},
			padding: 300,
			want:    []Interval{{From: 640, To: 1343}},
		},
		{
			name: "orders and merges what overlaps after the snap",
			in:   []Interval{{From: 5000, To: 5100}, {From: 1000, To: 1000}, {From: 1030, To: 1100}},
			want: []Interval{{From: 960, To: 1151}, {From: 4992, To: 5119}},
		},
		{
			name: "merges ranges that become adjacent",
			in:   []Interval{{From: 100, To: 127}, {From: 128, To: 200}},
			want: []Interval{{From: 64, To: 255}},
		},
		{
			name: "clamps the low edge to the first ledger",
			in:   []Interval{{From: 10, To: 20}},
			want: []Interval{{From: 1, To: 63}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizePlan(tc.in, tc.padding)
			if len(got) != len(tc.want) {
				t.Fatalf("normalizePlan() = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("interval %d = %v, want %v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestNormalizePlanIsIdempotent(t *testing.T) {
	once := normalizePlan([]Interval{{From: 5000, To: 5100}, {From: 1000, To: 1000}}, 300)
	twice := normalizePlan(once, 0)
	if fmt.Sprint(once) != fmt.Sprint(twice) {
		t.Errorf("normalizing an already normalized plan moved it: %v then %v", once, twice)
	}
}

func TestPartitionCoversTheRangeExactlyOnce(t *testing.T) {
	cases := []struct {
		name     string
		from, to uint32
		plan     []Interval
		want     []piece
	}{
		{
			name: "no plan defers everything",
			from: 100, to: 200,
			want: []piece{{from: 100, to: 200, mode: HealModeDeferred}},
		},
		{
			name: "a plan covering the range replays everything",
			from: 100, to: 200,
			plan: []Interval{{From: 1, To: 500}},
			want: []piece{{from: 100, to: 200, mode: HealModeReplay}},
		},
		{
			name: "a cluster in the middle leaves a desert on each side",
			from: 100, to: 200,
			plan: []Interval{{From: 140, To: 160}},
			want: []piece{
				{from: 100, to: 139, mode: HealModeDeferred},
				{from: 140, to: 160, mode: HealModeReplay},
				{from: 161, to: 200, mode: HealModeDeferred},
			},
		},
		{
			name: "intervals outside the range are ignored",
			from: 100, to: 200,
			plan: []Interval{{From: 1, To: 50}, {From: 190, To: 400}, {From: 900, To: 1000}},
			want: []piece{
				{from: 100, to: 189, mode: HealModeDeferred},
				{from: 190, to: 200, mode: HealModeReplay},
			},
		},
		{
			name: "a single ledger range",
			from: 100, to: 100,
			plan: []Interval{{From: 100, To: 100}},
			want: []piece{{from: 100, to: 100, mode: HealModeReplay}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := partition(tc.from, tc.to, tc.plan)
			if len(got) != len(tc.want) {
				t.Fatalf("partition() = %+v, want %+v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("piece %d = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
			// Whatever the plan says, a gap never loses a ledger to a split:
			// the pieces tile the range end to end.
			if got[0].from != tc.from {
				t.Errorf("first piece starts at %d, want %d", got[0].from, tc.from)
			}
			if last := got[len(got)-1]; last.to != tc.to {
				t.Errorf("last piece ends at %d, want %d", last.to, tc.to)
			}
			for i := 1; i < len(got); i++ {
				if got[i].from != got[i-1].to+1 {
					t.Errorf("pieces %d and %d are not contiguous: %+v", i-1, i, got)
				}
				if got[i].mode == got[i-1].mode {
					t.Errorf("pieces %d and %d share a mode and should have merged: %+v", i-1, i, got)
				}
			}
		})
	}
}
