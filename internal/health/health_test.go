package health

import "testing"

func TestGapsPendingHealIsTheCompletionSignal(t *testing.T) {
	cases := []struct {
		name              string
		open, deferred    int64
		want              int64
		wantZeroMeansDone bool
	}{
		{
			name: "no plan applied: every open gap is owed",
			open: 3, deferred: 0, want: 3,
		},
		{
			name: "a plan splits the work: only the replay side is owed",
			open: 116, deferred: 58, want: 58,
		},
		{
			// The whole point: open_gaps stays at 58 forever here, so
			// anything watching it for completion would never fire.
			name: "everything replayable is healed, deserts remain",
			open: 58, deferred: 58, want: 0, wantZeroMeansDone: true,
		},
		{
			name: "nothing left at all",
			open: 0, deferred: 0, want: 0, wantZeroMeansDone: true,
		},
		{
			// The two counters are polled separately, so a reading taken
			// between them can disagree. It must not report a negative.
			name: "counters read mid-refresh never go negative",
			open: 2, deferred: 5, want: 0, wantZeroMeansDone: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &State{}
			s.SetOpenGaps(tc.open)
			s.SetDeferredGaps(tc.deferred, 0)
			if got := s.gapsPendingHeal(); got != tc.want {
				t.Errorf("gapsPendingHeal() = %d, want %d", got, tc.want)
			}
			if tc.wantZeroMeansDone && s.gapsPendingHeal() != 0 {
				t.Error("the healer owes nothing here; the completion signal must read zero")
			}
		})
	}
}

func TestSetDeferredGapsFeedsStatus(t *testing.T) {
	s := &State{}
	s.SetOpenGaps(116)
	s.SetDeferredGaps(58, 5_050_212)

	srv := &Server{state: s}
	got := srv.snapshot()
	if got.OpenGaps != 116 || got.DeferredGaps != 58 || got.DeferredLedgers != 5_050_212 {
		t.Errorf("status = %+v, want the open, deferred and ledger counts as set", got)
	}
	if got.GapsPendingHeal != 58 {
		t.Errorf("gaps_pending_heal = %d, want 58", got.GapsPendingHeal)
	}
}
