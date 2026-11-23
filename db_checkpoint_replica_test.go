package litestream

import (
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/superfly/ltx"
)

// gatedReplicaClient blocks level-0 uploads until release is closed so tests
// can model a replica that is slow to consume local LTX files.
type gatedReplicaClient struct {
	*testReplicaClient

	mu      sync.Mutex
	blocked bool
	release chan struct{}
}

func newGatedReplicaClient(t *testing.T) *gatedReplicaClient {
	t.Helper()
	return &gatedReplicaClient{
		testReplicaClient: &testReplicaClient{dir: t.TempDir()},
		release:           make(chan struct{}),
	}
}

func (c *gatedReplicaClient) block() {
	c.mu.Lock()
	c.blocked = true
	c.mu.Unlock()
}

func (c *gatedReplicaClient) unblock() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blocked {
		c.blocked = false
		close(c.release)
	}
}

func (c *gatedReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.mu.Lock()
	blocked, release := c.blocked, c.release
	c.mu.Unlock()

	if blocked {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-release:
		}
	}
	return c.testReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func openCheckpointReplicaTestDB(t *testing.T, client ReplicaClient) (*DB, *sql.DB) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "db")
	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
	db.MinCheckpointPageN = 1
	db.Replica = NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = true
	db.Replica.SyncInterval = 5 * time.Millisecond
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	for _, pragma := range []string{
		`PRAGMA journal_mode = wal;`,
		`PRAGMA wal_autocheckpoint = 0;`,
		`PRAGMA busy_timeout = 5000;`,
	} {
		if _, err := sqldb.Exec(pragma); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB);`); err != nil {
		t.Fatal(err)
	}
	return db, sqldb
}

func waitForReplicaTXID(t *testing.T, r *Replica, target ltx.TXID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.Pos().TXID >= target {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("replica TXID=%s, want at least %s", r.Pos().TXID, target)
}

func TestDB_CheckpointDefersUntilReplicaCatchesUp(t *testing.T) {
	ctx := context.Background()
	client := newGatedReplicaClient(t)
	db, sqldb := openCheckpointReplicaTestDB(t, client)

	for i := 0; i < 20; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (data) VALUES (randomblob(4000));`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	waitForReplicaTXID(t, db.Replica, pos.TXID)

	// Let any checkpoint deferred during the initial capture run now that the
	// replica has caught up.
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	passiveBaseline := testutil.ToFloat64(checkpointNCounterVec.WithLabelValues(db.Path(), CheckpointModePassive))

	client.block()
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (data) VALUES (randomblob(4000));`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	pos, err = db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	if db.Replica.Pos().TXID >= pos.TXID {
		t.Fatalf("precondition: replica TXID=%s must lag local TXID=%s", db.Replica.Pos().TXID, pos.TXID)
	}
	if got := testutil.ToFloat64(checkpointNCounterVec.WithLabelValues(db.Path(), CheckpointModePassive)); got != passiveBaseline {
		t.Fatalf("checkpoint ran before replica caught up: passive checkpoints=%v, want %v", got, passiveBaseline)
	}

	client.unblock()
	waitForReplicaTXID(t, db.Replica, pos.TXID)
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(checkpointNCounterVec.WithLabelValues(db.Path(), CheckpointModePassive)); got <= passiveBaseline {
		t.Fatalf("checkpoint did not resume after replica caught up: passive checkpoints=%v, want > %v", got, passiveBaseline)
	}

	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	restorePath := filepath.Join(t.TempDir(), "restored.db")
	opt := NewRestoreOptions()
	opt.OutputPath = restorePath
	opt.IntegrityCheck = IntegrityCheckFull
	if err := db.Replica.Restore(ctx, opt); err != nil {
		t.Fatal(err)
	}
	restored, err := sql.Open("sqlite", restorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var want, got int
	if err := sqldb.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&want); err != nil {
		t.Fatal(err)
	}
	if err := restored.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("restored rows=%d, want %d", got, want)
	}
}

func TestDB_TruncateCheckpointWaitsForReplica(t *testing.T) {
	ctx := context.Background()
	client := newGatedReplicaClient(t)
	db, sqldb := openCheckpointReplicaTestDB(t, client)

	for i := 0; i < 20; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (data) VALUES (randomblob(4000));`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	waitForReplicaTXID(t, db.Replica, pos.TXID)

	client.block()
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (data) VALUES (randomblob(4000));`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos, err = db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	truncateBaseline := testutil.ToFloat64(checkpointNCounterVec.WithLabelValues(db.Path(), CheckpointModeTruncate))
	done := make(chan error, 1)
	go func() { done <- db.Checkpoint(ctx, CheckpointModeTruncate) }()

	select {
	case err := <-done:
		t.Fatalf("truncate checkpoint completed before replica consumed TXID %s: %v", pos.TXID, err)
	case <-time.After(150 * time.Millisecond):
	}

	client.unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("truncate checkpoint did not resume after replica caught up")
	}
	if got := testutil.ToFloat64(checkpointNCounterVec.WithLabelValues(db.Path(), CheckpointModeTruncate)); got <= truncateBaseline {
		t.Fatalf("truncate checkpoints=%v, want > %v", got, truncateBaseline)
	}

	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	restorePath := filepath.Join(t.TempDir(), "restored.db")
	opt := NewRestoreOptions()
	opt.OutputPath = restorePath
	opt.IntegrityCheck = IntegrityCheckFull
	if err := db.Replica.Restore(ctx, opt); err != nil {
		t.Fatal(err)
	}
	restored, err := sql.Open("sqlite", restorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var want, got int
	if err := sqldb.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&want); err != nil {
		t.Fatal(err)
	}
	if err := restored.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("restored rows=%d, want %d", got, want)
	}
}

func TestDB_EmergencyTruncateCheckpointDoesNotWaitForReplica(t *testing.T) {
	ctx := context.Background()
	client := newGatedReplicaClient(t)
	db, sqldb := openCheckpointReplicaTestDB(t, client)

	for i := 0; i < 20; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (data) VALUES (randomblob(4000));`); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	waitForReplicaTXID(t, db.Replica, pos.TXID)

	db.TruncatePageN = 2
	client.block()
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (data) VALUES (randomblob(4000));`); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- db.Sync(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("emergency checkpoint waited for the replica instead of bounding WAL growth")
	}
	client.unblock()
}
