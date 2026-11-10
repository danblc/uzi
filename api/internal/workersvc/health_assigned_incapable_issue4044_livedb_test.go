package workersvc

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/vtmocanu/uzi/api/internal/store"
)

// Issue #4044 LiveDB coverage: a queued continuation that needs repo_agent_source_v1 and is
// assigned to an online, draining worker WITHOUT that capability can never be claimed by that
// worker (ClaimRun's standalone repo-agent-source clause is not overridden by the drain
// allowance). Health used to report the fleet-wide "all your workers are busy" text. These tests
// drive the real ListActiveRunsForHealth projection, queuedReason and ClaimRun over a throwaway
// Postgres (skipped unless UZI_TEST_DATABASE_URL). Fixture worker names avoid builtin template names.

const (
	issue4044RepoCap = "repo_agent_source_v1"
	issue4044Ceiling = 25 * time.Minute // equals the AffinityCutoff window claimParams uses
	issue4044Commit  = "0123456789abcdef0123456789abcdef01234567"
	issue4044OldName = "old-roll-4044"
)

// issue4044Opts shapes one fixture. A zero value is the reported failure: the old worker is
// draining, lacks the capability, has one active run of two, and the queued continuation is past
// the affinity ceiling; peers are added explicitly.
type issue4044Opts struct {
	oldCaps     []string
	oldMax      int
	notDraining bool
	required    bool
	commit      bool
	age         time.Duration // run updated_at = now - age
	noOldActive bool
}

type issue4044Env struct {
	e   interlockLiveDB
	svc *Service
	old uuid.UUID
	run uuid.UUID
}

func newIssue4044(t *testing.T, o issue4044Opts) *issue4044Env {
	t.Helper()
	e := setupInterlockLiveDB(t)
	svc := e.permitService(t)
	svc.SetCapabilitySettings(fakeCapabilitySettings{on: true})
	svc.p.WorkerAffinityCeiling = issue4044Ceiling
	svc.p.WorkerStaleRequeueGrace = 0
	if o.oldMax == 0 {
		o.oldMax = 2
	}
	if o.age == 0 {
		o.age = time.Hour
	}
	caps := o.oldCaps
	if caps == nil {
		caps = []string{}
	}
	old := uuid.New()
	var drainingSince any
	var drainingReason any
	if !o.notDraining {
		drainingSince = time.Now().Add(-time.Hour)
		drainingReason = "image_roll"
	}
	e.exec(t, `INSERT INTO workers (id,user_id,name,token_hash,status,protocol_capabilities,max_concurrent_runs,
		last_heartbeat_at,draining_since,draining_reason)
		VALUES ($1,$2,$3,$4,'online',$5,$6,now(),$7,$8)`,
		old, e.userID, issue4044OldName, old[:], caps, o.oldMax, drainingSince, drainingReason)
	if !o.noOldActive {
		e.seedActiveRunOwnedBy(t, old)
	}
	run := uuid.New()
	iid := *e.nextIID
	*e.nextIID++
	var commit any
	if o.commit {
		commit = issue4044Commit
	}
	e.exec(t, `INSERT INTO runs (id,user_id,repo_id,issue_iid,issue_title,issue_description,kind,status,worker_id,
		repo_agent_source_required,repo_agent_source_commit,claim_generation,status_since,updated_at,created_at)
		VALUES ($1,$2,$3,$4,'t','d','issue','queued',$5,$6,$7,9,$8,$8,$8)`,
		run, e.userID, e.repoID, iid, old, o.required, commit, time.Now().Add(-o.age))
	return &issue4044Env{e: e, svc: svc, old: old, run: run}
}

// addPeer inserts an online peer with an advertised cap and `active` running runs.
func (f *issue4044Env) addPeer(t *testing.T, name, status string, caps []string, maxRuns, active int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if caps == nil {
		caps = []string{}
	}
	f.e.exec(t, `INSERT INTO workers (id,user_id,name,token_hash,status,protocol_capabilities,max_concurrent_runs,last_heartbeat_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,now())`, id, f.e.userID, name, id[:], status, caps, maxRuns)
	for i := 0; i < active; i++ {
		f.e.seedActiveRunOwnedBy(t, id)
	}
	return id
}

func (f *issue4044Env) reason(t *testing.T) string {
	t.Helper()
	return f.svc.queuedReason(f.e.ctx, time.Now(), f.e.healthRowFor(t, f.run), thWithLimit(testCustodyLimit))
}

func (f *issue4044Env) commitIntact(t *testing.T) {
	t.Helper()
	var got pgtype.Text
	if err := f.e.pool.QueryRow(f.e.ctx, `SELECT repo_agent_source_commit FROM runs WHERE id=$1`, f.run).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Valid || got.String != issue4044Commit {
		t.Fatalf("repo_agent_source_commit = %+v, want %q intact", got, issue4044Commit)
	}
}

func (f *issue4044Env) claimAs(w uuid.UUID, caps []string, draining bool) (store.Run, error) {
	p := f.e.claimParams(w, caps, false)
	p.ClaimantDraining = draining
	return f.e.q.ClaimRun(f.e.ctx, p)
}

const issue4044BusyWant = "its previous worker " + issue4044OldName +
	" lacks repo_agent_source_v1, which this run requires, so it cannot resume it; " +
	"the capable workers that may take it are busy; waiting for a free slot on one"

// Regression: the reported shape must not read as a saturated fleet.
func TestIssue4044AssignedIncapableDrainingWorkerIsNotReportedBusyLiveDB(t *testing.T) {
	f := newIssue4044(t, issue4044Opts{required: true, commit: true})
	f.addPeer(t, "peer-cap-4044", "online", []string{issue4044RepoCap}, 1, 1)
	got := f.reason(t)
	if got == reasonAllWorkersBusy {
		t.Fatalf("reason = the generic busy text %q for a run its own worker cannot resume", got)
	}
	for _, want := range []string{"repo_agent_source_v1", issue4044OldName, "cannot resume"} {
		if !strings.Contains(got, want) {
			t.Fatalf("reason %q lacks %q", got, want)
		}
	}
	f.commitIntact(t)
}

func TestIssue4044ClaimControlsLiveDB(t *testing.T) {
	t.Run("old worker without the capability is refused, with it claims its own run", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true})
		if _, err := f.claimAs(f.old, []string{}, true); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("draining old worker without cap: err = %v, want no rows", err)
		}
		f.commitIntact(t)
		// Same fixture, capability added to the row and to the claim params.
		f.e.exec(t, `UPDATE workers SET protocol_capabilities = $2 WHERE id = $1`, f.old, []string{issue4044RepoCap})
		run, err := f.claimAs(f.old, []string{issue4044RepoCap}, true)
		if err != nil || run.ID != f.run {
			t.Fatalf("draining old worker with cap: run %v err %v, want %v", run.ID, err, f.run)
		}
	})
	t.Run("peer waits for the ceiling and needs the capability", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true, age: 5 * time.Minute})
		peer := f.addPeer(t, "peer-claim-4044", "online", []string{issue4044RepoCap}, 3, 0)
		// Before the ceiling a capable peer must not take it.
		if _, err := f.claimAs(peer, []string{issue4044RepoCap}, false); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("capable peer before ceiling: err = %v, want no rows", err)
		}
		f.e.exec(t, `UPDATE runs SET updated_at = now() - interval '1 hour' WHERE id = $1`, f.run)
		// Past it, a peer lacking the capability is still refused.
		f.e.exec(t, `UPDATE workers SET protocol_capabilities = '{}' WHERE id = $1`, peer)
		if _, err := f.claimAs(peer, []string{}, false); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("incapable peer past ceiling: err = %v, want no rows", err)
		}
		f.commitIntact(t)
		f.e.exec(t, `UPDATE workers SET protocol_capabilities = $2 WHERE id = $1`, peer, []string{issue4044RepoCap})
		run, err := f.claimAs(peer, []string{issue4044RepoCap}, false)
		if err != nil || run.ID != f.run {
			t.Fatalf("capable peer past ceiling: run %v err %v", run.ID, err)
		}
		if run.ClaimGeneration != 10 || uuid.UUID(run.WorkerID.Bytes) != peer || run.Status != "claimed" {
			t.Fatalf("claimed run generation=%d worker=%v status=%q, want 10/%v/claimed", run.ClaimGeneration, uuid.UUID(run.WorkerID.Bytes), run.Status, peer)
		}
		if !run.RepoAgentSourceCommit.Valid || run.RepoAgentSourceCommit.String != issue4044Commit {
			t.Fatalf("claimed run commit = %+v, want unchanged %q", run.RepoAgentSourceCommit, issue4044Commit)
		}
	})
	t.Run("commit without the required flag is refused too", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{commit: true})
		if _, err := f.claimAs(f.old, []string{}, true); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("commit-only run claimed by incapable old worker: err = %v, want no rows", err)
		}
		f.commitIntact(t)
	})
}

func TestIssue4044HealthVariantsLiveDB(t *testing.T) {
	t.Run("capable peer at its cap reports the busy variant", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true})
		f.addPeer(t, "peer-busy-4044", "online", []string{issue4044RepoCap}, 1, 1)
		got := f.reason(t)
		if got != issue4044BusyWant {
			t.Fatalf("reason = %q, want %q", got, issue4044BusyWant)
		}
		if got == reasonNoRepoAgentSourceCapableWorker {
			t.Fatalf("a capable peer is online; the no-capable-worker reason must not fire")
		}
		f.commitIntact(t)
	})
	t.Run("capable peer with a free slot is waiting to claim", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true})
		f.addPeer(t, "peer-free-4044", "online", []string{issue4044RepoCap}, 2, 1)
		want := "its previous worker " + issue4044OldName +
			" lacks repo_agent_source_v1, which this run requires, so it cannot resume it; waiting for a capable worker to claim it"
		if got := f.reason(t); got != want {
			t.Fatalf("reason = %q, want %q", got, want)
		}
	})
	t.Run("offline free capable peer does not hide the busy online one", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true})
		f.addPeer(t, "peer-busy-4044", "online", []string{issue4044RepoCap}, 1, 1)
		f.addPeer(t, "peer-off-4044", "offline", []string{issue4044RepoCap}, 2, 0)
		counts, err := f.svc.workerEligibilityForHealth(f.e.ctx, time.Now(), f.run, true)
		if err != nil {
			t.Fatal(err)
		}
		if counts.Claimable == 0 {
			t.Fatalf("fixture must keep the legacy Claimable count above zero, got %+v", counts)
		}
		if got := f.reason(t); got != issue4044BusyWant {
			t.Fatalf("reason = %q, want %q", got, issue4044BusyWant)
		}
	})
	t.Run("no capable peer keeps the specific no-capable-worker reason", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true})
		f.addPeer(t, "peer-nocap-4044", "online", nil, 2, 0)
		if got := f.reason(t); got != reasonNoRepoAgentSourceCapableWorker {
			t.Fatalf("reason = %q, want %q", got, reasonNoRepoAgentSourceCapableWorker)
		}
	})
	t.Run("before the ceiling the owner hold names the incapable worker", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true, age: 5 * time.Minute})
		f.addPeer(t, "peer-busy-4044", "online", []string{issue4044RepoCap}, 1, 1)
		row := f.e.healthRowFor(t, f.run)
		until := row.UpdatedAt.Time.Add(issue4044Ceiling).UTC().Format(time.RFC3339)
		want := "waiting until " + until + " for its previous worker " + issue4044OldName +
			", which lacks repo_agent_source_v1 that this run requires and cannot resume it; a capable worker may take it after that"
		got := f.svc.queuedReason(f.e.ctx, time.Now(), row, thWithLimit(testCustodyLimit))
		if got != want {
			t.Fatalf("reason = %q, want %q", got, want)
		}
		if !isStaleRequeuePinReason(got) {
			t.Fatalf("%q must count as an expected owner hold", got)
		}
		f.commitIntact(t)
	})
	t.Run("a capable draining owner keeps the existing hold text", func(t *testing.T) {
		// One lane, already used: the owner cannot take its own run, so the draining hold applies.
		f := newIssue4044(t, issue4044Opts{oldCaps: []string{issue4044RepoCap}, oldMax: 1, required: true, commit: true, age: 5 * time.Minute})
		f.addPeer(t, "peer-busy-4044", "online", []string{issue4044RepoCap}, 1, 1)
		row := f.e.healthRowFor(t, f.run)
		until := row.UpdatedAt.Time.Add(issue4044Ceiling).UTC().Format(time.RFC3339)
		want := "waiting until " + until + " for its previous worker " + issue4044OldName +
			" to finish draining or maintenance; another worker may take it after that"
		if got := f.svc.queuedReason(f.e.ctx, time.Now(), row, thWithLimit(testCustodyLimit)); got != want {
			t.Fatalf("reason = %q, want %q", got, want)
		}
	})
	t.Run("no online worker still wins", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true})
		f.e.exec(t, `UPDATE workers SET status = 'offline', last_heartbeat_at = now() - interval '1 day' WHERE id = $1`, f.old)
		if got := f.reason(t); got != reasonNoWorker {
			t.Fatalf("reason = %q, want %q", got, reasonNoWorker)
		}
		f.commitIntact(t)
	})
	t.Run("a manual cordon on the only capable peer still wins", func(t *testing.T) {
		f := newIssue4044(t, issue4044Opts{required: true, commit: true})
		peer := f.addPeer(t, "peer-cordon-4044", "online", []string{issue4044RepoCap}, 2, 0)
		if _, err := store.ChangeWorkerManualCordon(f.e.ctx, f.e.pool, f.e.userID, peer, "lane operator", pgtype.Text{}, true); err != nil {
			t.Fatal(err)
		}
		if got := f.reason(t); !strings.HasPrefix(got, manualCordonWaitPrefix) {
			t.Fatalf("reason = %q, want the manual cordon wait", got)
		}
	})
}

// Issue #4047 shape: the flag is set but no commit is recorded yet.
func TestIssue4044RequiredWithoutCommitLiveDB(t *testing.T) {
	f := newIssue4044(t, issue4044Opts{required: true})
	f.addPeer(t, "peer-busy-4044", "online", []string{issue4044RepoCap}, 1, 1)
	got := f.reason(t)
	if got != issue4044BusyWant {
		t.Fatalf("reason = %q, want %q", got, issue4044BusyWant)
	}
	rows, err := f.e.q.ListWorkerUpgradeBlockers(f.e.ctx, []uuid.UUID{f.old})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range rows {
		if r.RunID.Valid && uuid.UUID(r.RunID.Bytes) == f.run {
			found = true
			if !r.MissingRepoAgentSource {
				t.Fatalf("blocker row for the queued run: MissingRepoAgentSource = false: %+v", r)
			}
		}
	}
	if !found {
		t.Fatalf("queued run not among blocker rows: %+v", rows)
	}
}
