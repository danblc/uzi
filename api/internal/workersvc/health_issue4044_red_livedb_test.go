package workersvc

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vtmocanu/uzi/api/internal/store"
)

// Issue #4044 red-proof LiveDB tests. Deliberately portable to the base commit: they use only
// APIs that already existed there (the interlockLiveDB harness, raw SQL fixtures, the real
// ListActiveRunsForHealth row and Service.queuedReason) and string literals for expectations.
//
// Shape: a queued continuation whose assigned worker A lacks repo_agent_source_v1 (the run requires
// it, by flag and/or a recorded commit). ClaimRun refuses A on capability, so the queued reason must
// name that gap instead of "all your workers are busy" / "waiting for a worker to pick up this run".

const (
	red4044GenericBusy    = "all your workers are busy; this run is waiting for a free slot"
	red4044GenericWaiting = "waiting for a worker to pick up this run"
)

type red4044Fixture struct {
	e      interlockLiveDB
	svc    *Service
	aID    uuid.UUID
	aName  string
	runID  uuid.UUID
	commit string
}

// red4044Seed creates the draining incapable worker A (cap 2, one running run, fresh heartbeat) and
// the queued continuation assigned to it. commit "" leaves repo_agent_source_commit NULL.
// pastCeiling backdates updated_at beyond the service's affinity ceiling.
func red4044Seed(t *testing.T, e interlockLiveDB, commit string, pastCeiling bool) red4044Fixture {
	t.Helper()
	svc := e.permitService(t)
	aID := uuid.New()
	aName := "a-" + aID.String()[:8]
	e.exec(t, `INSERT INTO workers (id, user_id, name, token_hash, status, protocol_capabilities, max_concurrent_runs,
	           last_heartbeat_at, draining_since, draining_reason)
	           VALUES ($1, $2, $3, $4, 'online', $5, 2, now(), now(), 'image_roll')`,
		aID, e.userID, aName, aID[:], []string{"completion_interlock_v1"})
	e.seedActiveRunOwnedBy(t, aID)

	runID := uuid.New()
	iid := *e.nextIID
	*e.nextIID++
	var commitArg any
	if commit != "" {
		commitArg = commit
	}
	e.exec(t, `INSERT INTO runs (id, user_id, repo_id, issue_iid, issue_title, issue_description, kind, status, worker_id,
	           claim_generation, repo_agent_source_required, repo_agent_source_commit)
	           VALUES ($1, $2, $3, $4, 't', 'd', 'issue', 'queued', $5, 9, true, $6)`,
		runID, e.userID, e.repoID, iid, aID, commitArg)
	if pastCeiling {
		e.exec(t, `UPDATE runs SET updated_at = now() - ($2 * interval '1 second') WHERE id = $1`,
			runID, (svc.p.WorkerAffinityCeiling + time.Hour).Seconds())
	}
	return red4044Fixture{e: e, svc: svc, aID: aID, aName: aName, runID: runID, commit: commit}
}

// fullCapablePeer seeds an online v1 worker with cap 1 and one running run (no free slot).
func (f red4044Fixture) fullCapablePeer(t *testing.T) uuid.UUID {
	t.Helper()
	b := f.e.seedSpreadWorker(t, []string{"repo_agent_source_v1"}, 1)
	f.e.seedActiveRunOwnedBy(t, b)
	return b
}

// freeCapablePeer seeds an online v1 worker with cap 2 and one running run (one free slot).
func (f red4044Fixture) freeCapablePeer(t *testing.T) uuid.UUID {
	t.Helper()
	b := f.e.seedSpreadWorker(t, []string{"repo_agent_source_v1"}, 2)
	f.e.seedActiveRunOwnedBy(t, b)
	return b
}

// reason reads the real health row for the run and resolves its queued reason.
func (f red4044Fixture) reason(t *testing.T) string {
	t.Helper()
	rows, err := f.e.q.ListActiveRunsForHealth(f.e.ctx, codexCuratedModelsSlice())
	if err != nil {
		t.Fatalf("ListActiveRunsForHealth: %v", err)
	}
	var row *store.ListActiveRunsForHealthRow
	for i := range rows {
		if rows[i].ID == f.runID {
			row = &rows[i]
		}
	}
	if row == nil {
		t.Fatalf("run %v missing from the health projection", f.runID)
	}
	return f.svc.queuedReason(f.e.ctx, time.Now(), *row, thWithLimit(testCustodyLimit))
}

func red4044AssertNamesGap(t *testing.T, got string, inWindow bool) {
	t.Helper()
	for _, want := range []string{"repo_agent_source_v1", "cannot resume this run"} {
		if !strings.Contains(got, want) {
			t.Fatalf("queued reason %q does not contain %q", got, want)
		}
	}
	anchor := "assigned worker"
	if inWindow {
		anchor = "previous worker"
	}
	if !strings.Contains(got, anchor) {
		t.Fatalf("queued reason %q does not contain %q", got, anchor)
	}
	if got == red4044GenericBusy || got == red4044GenericWaiting {
		t.Fatalf("queued reason is the generic %q, hiding the capability gap", got)
	}
}

func TestIssue4044AssignedWorkerLacksRepoAgentSourceNamedLiveDB(t *testing.T) {
	commit := strings.Repeat("a", 40)
	t.Run("full capable peer", func(t *testing.T) {
		e := setupInterlockLiveDB(t)
		f := red4044Seed(t, e, commit, true)
		f.fullCapablePeer(t)
		red4044AssertNamesGap(t, f.reason(t), false)
	})
	t.Run("free capable peer", func(t *testing.T) {
		e := setupInterlockLiveDB(t)
		f := red4044Seed(t, e, commit, true)
		f.freeCapablePeer(t)
		red4044AssertNamesGap(t, f.reason(t), false)
	})
	t.Run("free incapable worker and full capable peer", func(t *testing.T) {
		e := setupInterlockLiveDB(t)
		f := red4044Seed(t, e, commit, true)
		f.fullCapablePeer(t)
		// Free but incapable: it makes the generic fleet-wide free-slot count positive.
		e.seedSpreadWorker(t, []string{"completion_interlock_v1"}, 2)
		red4044AssertNamesGap(t, f.reason(t), false)
	})
	t.Run("required flag without a recorded commit", func(t *testing.T) {
		e := setupInterlockLiveDB(t)
		f := red4044Seed(t, e, "", true)
		f.fullCapablePeer(t)
		red4044AssertNamesGap(t, f.reason(t), false)
	})
	t.Run("inside the affinity window owner hold", func(t *testing.T) {
		e := setupInterlockLiveDB(t)
		f := red4044Seed(t, e, commit, false)
		f.fullCapablePeer(t)
		red4044AssertNamesGap(t, f.reason(t), true)
	})
}
