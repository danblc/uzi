package workersvc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/vtmocanu/uzi/api/internal/capability"
	"github.com/vtmocanu/uzi/api/internal/store"
)

// Issue #4044 unit coverage: the queued reason for a continuation whose assigned worker lacks
// repo_agent_source_v1. The capacity read is an optional interface on the store, so one wrapper
// carries the worker reads and a second adds the capacity count.

const (
	issue4044Gap = " does not report repo_agent_source_v1, which this run requires, so it cannot resume this run"
	// The owner-hold variant is spelled out here, not derived from the production constants.
	issue4044HoldGap = ", which does not report repo_agent_source_v1, which this run requires, so it cannot resume this run" +
		"; another worker may take it after that"
)

type assignedGapStore struct {
	*healthFakeStore
	assigned    store.Worker
	assignedErr error
	assignCalls []store.GetWorkerByIDForUserParams
	released    store.Worker
	repoCount   int64
	repoErr     error
}

func (s *assignedGapStore) GetWorkerByIDForUser(_ context.Context, arg store.GetWorkerByIDForUserParams) (store.Worker, error) {
	s.assignCalls = append(s.assignCalls, arg)
	return s.assigned, s.assignedErr
}

func (s *assignedGapStore) GetWorkerByID(context.Context, uuid.UUID) (store.Worker, error) {
	return s.released, nil
}

func (s *assignedGapStore) CountOnlineWorkersSatisfyingRepoAgentSource(context.Context, uuid.UUID) (int64, error) {
	return s.repoCount, s.repoErr
}

type assignedGapCapacityStore struct {
	*assignedGapStore
	row   store.CountOnlineWorkersRepoAgentSourceCapacityRow
	err   error
	calls []uuid.UUID
}

func (s *assignedGapCapacityStore) CountOnlineWorkersRepoAgentSourceCapacity(_ context.Context, userID uuid.UUID) (store.CountOnlineWorkersRepoAgentSourceCapacityRow, error) {
	s.calls = append(s.calls, userID)
	return s.row, s.err
}

// gapRun is an aged queued continuation assigned to a worker and requiring the capability.
func gapRun() store.ListActiveRunsForHealthRow {
	r := queuedRunPastThreshold()
	r.WorkerID = pgtype.UUID{Bytes: uuid.New(), Valid: true}
	r.RepoAgentSourceRequired = true
	return r
}

func gapStore(r store.ListActiveRunsForHealthRow) *assignedGapStore {
	return &assignedGapStore{
		healthFakeStore: &healthFakeStore{active: []store.ListActiveRunsForHealthRow{r}, onlineWorkers: 2},
		assigned:        store.Worker{ID: uuid.UUID(r.WorkerID.Bytes), Name: "wA", ProtocolCapabilities: []string{"completion_interlock_v1"}},
		repoCount:       1,
	}
}

func gapReason(t *testing.T, st Store, fs *healthFakeStore, r store.ListActiveRunsForHealthRow) string {
	t.Helper()
	svc := healthSvc(st, defaultHealthSettings())
	svc.detectRunHealth(context.Background(), t0)
	return lastWrite(t, fs, r.ID).HealthReason.String
}

func TestIssue4044AssignedGapNamesWorkerAndCapacity(t *testing.T) {
	for _, tc := range []struct {
		name      string
		row       store.CountOnlineWorkersRepoAgentSourceCapacityRow
		repoErr   error
		wantSufix string
	}{
		{"capable peers full", store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 1}, nil,
			"; a worker that reports it is online but has no free slot"},
		{"capable peer free", store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 2, CapableFree: 1}, nil,
			"; waiting for a worker that reports it to pick up this run"},
		{"earlier count failed", store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 1}, errors.New("boom"),
			"; a worker that reports it is online but has no free slot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gapRun()
			gs := gapStore(r)
			gs.repoErr = tc.repoErr
			cs := &assignedGapCapacityStore{assignedGapStore: gs, row: tc.row}
			got := gapReason(t, cs, gs.healthFakeStore, r)
			if want := "assigned worker wA" + issue4044Gap + tc.wantSufix; got != want {
				t.Fatalf("reason = %q, want %q", got, want)
			}
			if len(cs.calls) != 1 || cs.calls[0] != r.UserID {
				t.Fatalf("capacity calls = %v, want one for the run owner", cs.calls)
			}
			if len(gs.assignCalls) == 0 || gs.assignCalls[0].ID != uuid.UUID(r.WorkerID.Bytes) || gs.assignCalls[0].UserID != r.UserID {
				t.Fatalf("assigned worker lookup = %+v, want the run's worker scoped to its owner", gs.assignCalls)
			}
		})
	}
}

func TestIssue4044AssignedGapFallsThrough(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(gs *assignedGapStore) Store
	}{
		{"no capable worker per capacity", func(gs *assignedGapStore) Store {
			return &assignedGapCapacityStore{assignedGapStore: gs}
		}},
		{"capacity read fails", func(gs *assignedGapStore) Store {
			return &assignedGapCapacityStore{assignedGapStore: gs, err: errors.New("db down"),
				row: store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 3}}
		}},
		{"capacity method absent", func(gs *assignedGapStore) Store { return gs }},
		{"assigned worker reports the capability", func(gs *assignedGapStore) Store {
			gs.assigned.ProtocolCapabilities = []string{capability.RepoAgentSourceV1}
			return &assignedGapCapacityStore{assignedGapStore: gs, row: store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 1}}
		}},
		{"assigned worker read fails", func(gs *assignedGapStore) Store {
			gs.assignedErr = errors.New("gone")
			return &assignedGapCapacityStore{assignedGapStore: gs, row: store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 1}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gapRun()
			gs := gapStore(r)
			st := tc.setup(gs)
			if got := gapReason(t, st, gs.healthFakeStore, r); got != reasonAllWorkersBusy {
				t.Fatalf("reason = %q, want the generic %q", got, reasonAllWorkersBusy)
			}
		})
	}

	t.Run("run does not require the capability", func(t *testing.T) {
		r := gapRun()
		r.RepoAgentSourceRequired = false
		gs := gapStore(r)
		cs := &assignedGapCapacityStore{assignedGapStore: gs, row: store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 1}}
		if got := gapReason(t, cs, gs.healthFakeStore, r); got != reasonAllWorkersBusy {
			t.Fatalf("reason = %q, want %q", got, reasonAllWorkersBusy)
		}
		if len(gs.assignCalls) != 0 || len(cs.calls) != 0 {
			t.Fatalf("read the worker (%d) or capacity (%d) for a run that needs neither", len(gs.assignCalls), len(cs.calls))
		}
	})

	t.Run("recorded commit alone requires the capability", func(t *testing.T) {
		r := gapRun()
		r.RepoAgentSourceRequired = false
		r.RepoAgentSourceCommit = pgtype.Text{String: strings.Repeat("b", 40), Valid: true}
		gs := gapStore(r)
		cs := &assignedGapCapacityStore{assignedGapStore: gs, row: store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 1}}
		if got := gapReason(t, cs, gs.healthFakeStore, r); !strings.HasPrefix(got, "assigned worker wA") {
			t.Fatalf("reason = %q, want the assigned-worker gap", got)
		}
	})
}

func TestIssue4044AssignedGapKeepsEarlierRungs(t *testing.T) {
	capacity := store.CountOnlineWorkersRepoAgentSourceCapacityRow{Capable: 1}
	for _, tc := range []struct {
		name  string
		setup func(r *store.ListActiveRunsForHealthRow, gs *assignedGapStore)
		want  func() string
	}{
		{"custody limit", func(_ *store.ListActiveRunsForHealthRow, gs *assignedGapStore) {
			gs.custodyHolds = int64(testCustodyLimit)
		}, func() string { return reasonCustodyLimit }},
		{"deprioritized", func(r *store.ListActiveRunsForHealthRow, gs *assignedGapStore) {
			gs.priorityClass = map[uuid.UUID]string{r.ID: "background"}
		}, func() string { return reasonDeprioritized }},
		{"released worker", func(r *store.ListActiveRunsForHealthRow, gs *assignedGapStore) {
			r.ReleasedWorkerID = pgtype.UUID{Bytes: uuid.New(), Valid: true}
			gs.released = store.Worker{Name: "wR", LastHeartbeatAt: pgtype.Timestamptz{Time: t0, Valid: true}}
		}, func() string {
			return "waiting for another worker, or restart worker wR (its previous process was released at the time limit)"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gapRun()
			gs := gapStore(r)
			tc.setup(&r, gs)
			gs.active = []store.ListActiveRunsForHealthRow{r}
			cs := &assignedGapCapacityStore{assignedGapStore: gs, row: capacity}
			if got := gapReason(t, cs, gs.healthFakeStore, r); got != tc.want() {
				t.Fatalf("reason = %q, want %q", got, tc.want())
			}
		})
	}

	for _, automatic := range []bool{false, true} {
		t.Run(fmt.Sprintf("manual cordon automatic hold %v", automatic), func(t *testing.T) {
			r := ownerHeldGapRun()
			svc, gs, _ := ownerHoldGapSvc(r)
			actor := uuid.New()
			gs.eligibility = store.CountOnlineWorkersClaimableForRunRow{
				ManualSuitable: 1, ManualCordoned: 1, AutomaticOwnerHold: automatic,
				ManualContext: mustJSON(t, map[string]any{"actor_id": actor, "worker_id": uuid.New(), "at": t0, "actor_label": "op"}),
			}
			svc.q = &assignedGapCapacityStore{assignedGapStore: gs, row: capacity}
			svc.detectRunHealth(context.Background(), t0)
			got := lastWrite(t, gs.healthFakeStore, r.ID).HealthReason.String
			manual := fmt.Sprintf("waiting for workers cordoned (manual): op (%s) at %s", actor, t0.Format(time.RFC3339))
			if !automatic {
				if got != manual {
					t.Fatalf("reason = %q, want %q", got, manual)
				}
				return
			}
			if want := manual + "; " + issue4044HoldText; got != want {
				t.Fatalf("reason = %q, want %q", got, want)
			}
		})
	}
}

// ownerHeldGapRun is the owner-held fixture of health_owner_hold_test.go, assigned to a worker that
// requires the capability.
func ownerHeldGapRun() store.ListActiveRunsForHealthRow {
	r := ownerHeldRun()
	r.WorkerID = pgtype.UUID{Bytes: uuid.New(), Valid: true}
	r.RepoAgentSourceRequired = true
	r.StaleRequeueWorkerName = "wA"
	return r
}

func ownerHoldGapSvc(r store.ListActiveRunsForHealthRow) (*Service, *assignedGapStore, *fakeBroadcaster) {
	gs := gapStore(r)
	svc, b := pinSvc(gs.healthFakeStore, defaultHealthSettings())
	svc.q = gs
	svc.p.WorkerAffinityCeiling = time.Hour
	return svc, gs, b
}

const issue4044HoldText = "waiting until 2026-07-12T12:30:00Z for its previous worker wA" + issue4044HoldGap

func TestIssue4044OwnerHoldVariantStableNoNudge(t *testing.T) {
	r := ownerHeldGapRun()
	svc, gs, b := ownerHoldGapSvc(r)
	svc.detectRunHealth(context.Background(), t0)
	w := lastWrite(t, gs.healthFakeStore, r.ID)
	if w.Health != healthWaitingWorker || w.HealthReason.String != issue4044HoldText {
		t.Fatalf("health=(%q,%q), want waiting_worker/%q", w.Health, w.HealthReason.String, issue4044HoldText)
	}
	if w.HealthNotifiedAt.Valid || len(b.healthNudges) != 1 || b.healthNudges[0] {
		t.Fatalf("unexpected nudge: stamp=%v nudges=%v", w.HealthNotifiedAt, b.healthNudges)
	}
	r.Health, r.HealthReason, r.HealthSince = w.Health, w.HealthReason, w.HealthSince
	gs.active = []store.ListActiveRunsForHealthRow{r}
	if n := svc.detectRunHealth(context.Background(), t0.Add(time.Minute)); n != 0 {
		t.Fatalf("stable tick changed %d rows", n)
	}
}

// Same lifecycle as TestHealthOwnerHoldLifecycle, seeded with the gap variant text.
func TestIssue4044OwnerHoldVariantLifecycleParity(t *testing.T) {
	for _, mode := range []string{"expiry", "claim", "stale only"} {
		t.Run(mode, func(t *testing.T) {
			r := ownerHeldGapRun()
			r.StatusSince = ago(15 * time.Minute)
			r.Health = healthWaitingWorker
			r.HealthReason = pgtype.Text{String: issue4044HoldText, Valid: true}
			r.HealthSince = ago(10 * time.Minute)
			r.UpdatedAt = ago(time.Hour)
			now := t0.Add(time.Nanosecond)
			want := reasonNoWorker
			wantNudge := true
			wantSince := now
			switch mode {
			case "claim":
				r.Status = "running"
				r.StartedAt = ago(time.Minute)
				r.LastActivityAt = ago(time.Second)
				want = ""
				wantNudge = false
			case "stale only":
				r.OwnerHoldPinnable = false
				r.StaleRequeuePinnable = true
				r.UpdatedAt = ago(time.Minute)
				want = "waiting until 2026-07-12T12:05:00Z for its previous worker wA to return; another worker may take it after that"
				wantSince = r.HealthSince.Time
				wantNudge = false
			}
			svc, gs, b := ownerHoldGapSvc(r)
			gs.onlineWorkers = 0
			svc.detectRunHealth(context.Background(), now)
			w := lastWrite(t, gs.healthFakeStore, r.ID)
			if w.HealthReason.String != want {
				t.Fatalf("reason=%q want %q", w.HealthReason.String, want)
			}
			if mode == "claim" {
				if w.Health != healthOK || w.HealthSince.Valid {
					t.Fatalf("claim did not clear health: %v", w)
				}
			} else if !w.HealthSince.Time.Equal(wantSince) {
				t.Fatalf("since=%v want %v", w.HealthSince, wantSince)
			}
			if len(b.healthNudges) != 1 || b.healthNudges[0] != wantNudge {
				t.Fatalf("nudges=%v want %v", b.healthNudges, wantNudge)
			}
			if wantNudge && (!w.HealthNotifiedAt.Valid || !w.HealthNotifiedAt.Time.Equal(now)) {
				t.Fatal("genuine wait missing notification stamp")
			}
			if !wantNudge && w.HealthNotifiedAt.Valid {
				t.Fatal("expected wait stamped notification")
			}
		})
	}
}

func TestIssue4044OwnerHoldVariantFallsBackWhenWorkerReportsCapability(t *testing.T) {
	r := ownerHeldGapRun()
	svc, gs, _ := ownerHoldGapSvc(r)
	gs.assigned.ProtocolCapabilities = []string{capability.RepoAgentSourceV1}
	svc.detectRunHealth(context.Background(), t0)
	want := "waiting until 2026-07-12T12:30:00Z for its previous worker wA to finish draining or maintenance; another worker may take it after that"
	if got := lastWrite(t, gs.healthFakeStore, r.ID).HealthReason.String; got != want {
		t.Fatalf("reason = %q, want %q", got, want)
	}
}

func TestIssue4044IsStaleRequeuePinReasonRecognizesVariant(t *testing.T) {
	if !isStaleRequeuePinReason(issue4044HoldText) {
		t.Fatalf("owner-hold gap variant %q not recognized as an owner hold", issue4044HoldText)
	}
	codeWait := "waiting until 2026-07-12T12:30:00Z" + reasonCodeCrossCheckWaitingSuffix
	if isStaleRequeuePinReason(codeWait) {
		t.Fatalf("code cross-check wait %q recognized as an owner hold", codeWait)
	}
	// Prefix and the "previous worker" segment are still required.
	for _, bad := range []string{
		"waiting until x for a worker" + issue4044HoldGap,
		"claimed until x for its previous worker wA" + issue4044HoldGap,
	} {
		if isStaleRequeuePinReason(bad) {
			t.Fatalf("%q recognized as an owner hold", bad)
		}
	}
}
