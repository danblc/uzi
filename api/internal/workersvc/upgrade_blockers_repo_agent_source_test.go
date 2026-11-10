package workersvc

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/vtmocanu/uzi/api/internal/store"
)

func TestUpgradeBlockerQueuedMissingRepoAgentSource(t *testing.T) {
	row := store.ListWorkerUpgradeBlockersRow{
		Status: blockerText("queued"), HealthReason: blockerText("vault_locked"), HoldReason: blockerText("credential_disabled"),
		MissingRepoAgentSource: true,
	}
	got := blockerAction(row)
	for _, want := range []string{repoAgentSourceGapFragment, "a capable worker must claim it or this worker re-registers with it", "recorded reason vault_locked"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q missing %q", got, want)
		}
	}

	row.MissingRepoAgentSource = false
	got = blockerAction(row)
	if strings.Contains(got, repoAgentSourceGapFragment) || !strings.Contains(got, "own continuation must become claimable; recorded reason vault_locked; hold credential_disabled; exact fence unavailable, inspect run") {
		t.Fatalf("unflagged queued text changed: %q", got)
	}

	// Codex admission reasons stay masked on the gap path too.
	row.MissingRepoAgentSource = true
	row.HealthReason = pgtype.Text{String: "waiting: admission held", Valid: true}
	if got := blockerAction(row); !strings.Contains(got, "recorded reason waiting: admission held") {
		t.Fatalf("%q", got)
	}
}
