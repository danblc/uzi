package workersvc

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/vtmocanu/uzi/api/internal/store"
)

// Issue #4044 LiveDB coverage for the queued reason of a continuation whose assigned worker lacks
// repo_agent_source_v1. Reuses the red4044Fixture helpers from health_issue4044_red_livedb_test.go.
// The reason text is compared as literals so a wording regression cannot hide behind the constants.

const (
	issue4044NoFreeText = " does not report repo_agent_source_v1, which this run requires, so it cannot resume this run" +
		"; a worker that reports it is online but has no free slot"
	issue4044WaitingText = " does not report repo_agent_source_v1, which this run requires, so it cannot resume this run" +
		"; waiting for a worker that reports it to pick up this run"
)

type issue4044RunState struct {
	commit     string
	generation int64
	worker     uuid.UUID
	status     string
}

func (f red4044Fixture) state(t *testing.T) issue4044RunState {
	t.Helper()
	var s issue4044RunState
	var commit pgtype.Text
	var worker pgtype.UUID
	if err := f.e.pool.QueryRow(f.e.ctx,
		`SELECT repo_agent_source_commit, claim_generation, worker_id, status FROM runs WHERE id = $1`, f.runID).
		Scan(&commit, &s.generation, &worker, &s.status); err != nil {
		t.Fatalf("read run state: %v", err)
	}
	s.commit = commit.String
	s.worker = worker.Bytes
	return s
}

func (f red4044Fixture) assertUnchanged(t *testing.T, want issue4044RunState) {
	t.Helper()
	if got := f.state(t); got != want {
		t.Fatalf("refused claim mutated the run: got %+v, want %+v", got, want)
	}
}

func (f red4044Fixture) claimAs(workerID uuid.UUID, caps []string, draining bool) (store.Run, error) {
	p := f.e.claimParams(workerID, caps, false)
	p.ClaimantDraining = draining
	return f.e.q.ClaimRun(f.e.ctx, p)
}

func (f red4044Fixture) capacity(t *testing.T) store.CountOnlineWorkersRepoAgentSourceCapacityRow {
	t.Helper()
	c, err := f.e.q.CountOnlineWorkersRepoAgentSourceCapacity(f.e.ctx, f.e.userID)
	if err != nil {
		t.Fatalf("CountOnlineWorkersRepoAgentSourceCapacity: %v", err)
	}
	return c
}

func (f red4044Fixture) genericFree(t *testing.T) int64 {
	t.Helper()
	n, err := f.e.q.CountOnlineWorkersWithFreeSlotForUser(f.e.ctx, f.e.userID)
	if err != nil {
		t.Fatalf("CountOnlineWorkersWithFreeSlotForUser: %v", err)
	}
	return n
}

func TestIssue4044ObservedShapeLiveDB(t *testing.T) {
	e := setupInterlockLiveDB(t)
	f := red4044Seed(t, e, "abcdef0123456789abcdef0123456789abcdef01", true)
	f.fullCapablePeer(t)
	want := "assigned worker " + f.aName + issue4044NoFreeText
	if got := f.reason(t); got != want {
		t.Fatalf("queued reason = %q, want %q", got, want)
	}
}

// ClaimRun refuses the draining assigned worker on capability, and admits it once it reports the
// capability: the diagnosis is the claim's own refusal, not a guess.
func TestIssue4044AssignedWorkerClaimRefusedOnCapabilityLiveDB(t *testing.T) {
	commit := "abcdef0123456789abcdef0123456789abcdef01"
	aCaps := []string{"completion_interlock_v1"}

	e := setupInterlockLiveDB(t)
	f := red4044Seed(t, e, commit, false)
	before := f.state(t)
	if _, err := f.claimAs(f.aID, aCaps, true); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("draining A without repo_agent_source_v1 claimed its own continuation (err=%v)", err)
	}
	f.assertUnchanged(t, before)

	// Positive control: same shape, but A's stored row and claim params report the capability.
	e2 := setupInterlockLiveDB(t)
	c := red4044Seed(t, e2, commit, false)
	withCap := []string{"completion_interlock_v1", "repo_agent_source_v1"}
	c.e.exec(t, `UPDATE workers SET protocol_capabilities = $2 WHERE id = $1`, c.aID, withCap)
	run, err := c.claimAs(c.aID, withCap, true)
	if err != nil {
		t.Fatalf("draining A reporting repo_agent_source_v1 must claim its own continuation: %v", err)
	}
	if run.ID != c.runID || run.ClaimGeneration != 10 {
		t.Fatalf("claimed run %v generation %d, want %v generation 10", run.ID, run.ClaimGeneration, c.runID)
	}
}

func TestIssue4044CapablePeerLiveDB(t *testing.T) {
	commit := "abcdef0123456789abcdef0123456789abcdef01"
	bCaps := []string{"repo_agent_source_v1"}

	t.Run("peer is refused inside the affinity window", func(t *testing.T) {
		e := setupInterlockLiveDB(t)
		f := red4044Seed(t, e, commit, false)
		b := f.e.seedSpreadWorker(t, bCaps, 2)
		before := f.state(t)
		if _, err := f.claimAs(b, bCaps, false); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("capable peer claimed a continuation still inside the owner hold (err=%v)", err)
		}
		f.assertUnchanged(t, before)
	})

	t.Run("full then free peer past the ceiling", func(t *testing.T) {
		e := setupInterlockLiveDB(t)
		f := red4044Seed(t, e, commit, true)
		b := f.e.seedSpreadWorker(t, bCaps, 1)
		bRun := f.e.seedActiveRunOwnedBy(t, b)
		before := f.state(t)

		got := f.reason(t)
		if want := "assigned worker " + f.aName + issue4044NoFreeText; got != want {
			t.Fatalf("full peer: queued reason = %q, want %q", got, want)
		}
		if c := f.capacity(t); c.Capable != 1 || c.CapableFree != 0 {
			t.Fatalf("full peer: capacity = %+v, want capable=1 capable_free=0", c)
		}
		if n := f.genericFree(t); n != 0 {
			t.Fatalf("full peer: generic free-slot count = %d, want 0", n)
		}
		f.assertUnchanged(t, before)

		f.e.exec(t, `UPDATE runs SET status = 'completed' WHERE id = $1`, bRun)
		got = f.reason(t)
		if want := "assigned worker " + f.aName + issue4044WaitingText; got != want {
			t.Fatalf("free peer: queued reason = %q, want %q", got, want)
		}
		if c := f.capacity(t); c.Capable != 1 || c.CapableFree != 1 {
			t.Fatalf("free peer: capacity = %+v, want capable=1 capable_free=1", c)
		}

		run, err := f.claimAs(b, bCaps, false)
		if err != nil {
			t.Fatalf("capable peer past the ceiling must claim: %v", err)
		}
		if run.ID != f.runID || run.ClaimGeneration != before.generation+1 || run.WorkerID.Bytes != b ||
			run.RepoAgentSourceCommit.String != commit {
			t.Fatalf("peer claim: id=%v generation=%d worker=%v commit=%q", run.ID, run.ClaimGeneration, run.WorkerID.Bytes, run.RepoAgentSourceCommit.String)
		}
		if run.ClaimGeneration != 10 {
			t.Fatalf("claim_generation = %d, want 10", run.ClaimGeneration)
		}
	})
}

func TestIssue4044NoCapablePeerKeepsSpecificReasonLiveDB(t *testing.T) {
	e := setupInterlockLiveDB(t)
	f := red4044Seed(t, e, "abcdef0123456789abcdef0123456789abcdef01", true)
	if got := f.reason(t); got != reasonNoRepoAgentSourceCapableWorker {
		t.Fatalf("queued reason = %q, want %q", got, reasonNoRepoAgentSourceCapableWorker)
	}
}

// Case (e), added by a later milestone, goes here.
