//! [`Db`] — the sole writer connection handle: `Arc<tokio::sync::Mutex<Connection>>`
//! plus a label, WAL + foreign_keys pragmas, and versioned migrations.
//!
//! This type is **never** handed to a route handler directly — only
//! [`crate::storage::batch::BatchWriter`] (its single dedicated writer
//! thread) and the migration bootstrap at boot touch it. Every other reader
//! goes through [`crate::storage::pool::DbPool`] instead, which is why this
//! type carries only the blocking helpers a plain `std::thread` worker
//! needs (`read_blocking`/`write_blocking`), not `spawn_blocking`-wrapped
//! async ones.

use std::sync::Arc;
use std::time::Duration;

use rusqlite::Connection;
use tokio::sync::Mutex;
use tracing::info;

use crate::storage::config::DbConfig;
use crate::storage::migrations::MigrationRunner;
use crate::storage::writer_pragmas::{apply_writer_pragmas, WriterPragmaConfig};

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("open {path}: {source}")]
    Open { path: String, source: rusqlite::Error },
    #[error("pragma init: {0}")]
    Pragma(rusqlite::Error),
    #[error("migration: {0}")]
    Migration(rusqlite::Error),
    #[error("query: {0}")]
    Query(#[from] rusqlite::Error),
    #[error("worker panicked: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("pool: {0}")]
    Pool(String),
    /// [`crate::storage::read_pool::ReadPool::read_with_deadline`] (and the
    /// default-deadline path on [`crate::storage::read_pool::ReadPool::read`]
    /// / `read_labeled`) interrupted a statement that ran past its
    /// deadline. The connection itself is unharmed — SQLite's own
    /// `sqlite3_interrupt()` contract guarantees an interrupted statement
    /// never corrupts the connection — and has already been returned to
    /// the pool by the time this error reaches the caller.
    #[error("read '{label}' exceeded its deadline after {elapsed:?}")]
    Deadline { label: String, elapsed: Duration },
}

/// Cheaply-clonable SQLite handle. Clones share the underlying `Mutex`, so a
/// single physical connection backs every clone — this crate's single
/// writer, per the module doc comment above.
#[derive(Clone)]
pub struct Db {
    inner: Arc<Mutex<Connection>>,
    label: Arc<str>,
}

impl Db {
    /// Opens the database, applies WAL (file-backed only) + foreign_keys
    /// pragmas plus the default writer pragma budget
    /// ([`WriterPragmaConfig::default`] — busy timeout, cache size,
    /// `synchronous=NORMAL`, journal size limit, WAL autocheckpoint
    /// backstop). Does NOT run migrations — call [`Db::run_migrations`]
    /// separately once any caller-side backfill against the raw connection
    /// (if applicable) has had a chance to run.
    pub fn open(cfg: &DbConfig) -> Result<Self, DbError> {
        Self::open_with_writer_pragmas(cfg, &WriterPragmaConfig::default())
    }

    /// Same as [`Self::open`], with the writer-side pragma budget
    /// ([`WriterPragmaConfig`]) under caller control instead of the
    /// default. Use when a store's write pattern genuinely differs from
    /// the default's own assumptions — see that type's field docs for
    /// what each knob trades off. Most consumers want [`Self::open`].
    pub fn open_with_writer_pragmas(cfg: &DbConfig, pragmas: &WriterPragmaConfig) -> Result<Self, DbError> {
        let (conn, label) = match cfg {
            DbConfig::Path(path) => {
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    let _ = std::fs::create_dir_all(parent);
                }
                let conn = Connection::open(path).map_err(|e| DbError::Open {
                    path: path.display().to_string(),
                    source: e,
                })?;
                (conn, path.display().to_string())
            }
            DbConfig::InMemory => {
                let conn = Connection::open_in_memory().map_err(|e| DbError::Open {
                    path: ":memory:".into(),
                    source: e,
                })?;
                (conn, ":memory:".to_owned())
            }
        };

        // WAL, and the rest of the writer pragma budget, are only
        // meaningful for file-backed connections (in-memory databases
        // don't have a journal file, so `synchronous` / `journal_size_limit`
        // / `wal_autocheckpoint` have nothing to act on) — but
        // `busy_timeout` and `cache_size` are real per-connection settings
        // either way.
        if !cfg.is_in_memory() {
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(DbError::Pragma)?;
            apply_writer_pragmas(&conn, pragmas)?;
        } else {
            conn.pragma_update(None, "busy_timeout", pragmas.busy_timeout_ms)
                .map_err(DbError::Pragma)?;
            conn.pragma_update(None, "cache_size", pragmas.cache_size_kib)
                .map_err(DbError::Pragma)?;
        }
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::Pragma)?;

        info!(path = %label, "sqlite db opened");
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
            label: Arc::from(label.as_str()),
        })
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    /// Run a set of [`crate::storage::migrations::Migration`]s against this
    /// db. Safe to call from inside a tokio runtime — uses `spawn_blocking`
    /// internally so the mutex is never held across an `.await`.
    pub async fn run_migrations(&self, runner: MigrationRunner) -> Result<(), DbError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.blocking_lock();
            runner.run(&mut guard)
        })
        .await?
        .map_err(DbError::Migration)
    }

    /// Blocking read for plain sync threads (a batch writer's own worker
    /// thread, migration bootstrap before a tokio runtime exists, tests):
    /// waits on the mutex rather than requiring `spawn_blocking`. Calling
    /// this from inside an async task panics — see
    /// [`tokio::sync::Mutex::blocking_lock`]'s own contract.
    pub fn read_blocking<F, T>(&self, f: F) -> rusqlite::Result<T>
    where
        F: FnOnce(&Connection) -> rusqlite::Result<T>,
    {
        let guard = self.inner.blocking_lock();
        f(&guard)
    }

    /// Blocking write for plain sync threads — same discipline as
    /// [`Db::read_blocking`], but hands the closure a `&mut Connection` so
    /// it can open its own transaction.
    pub fn write_blocking<F, T>(&self, f: F) -> rusqlite::Result<T>
    where
        F: FnOnce(&mut Connection) -> rusqlite::Result<T>,
    {
        let mut guard = self.inner.blocking_lock();
        f(&mut guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrations::Migration;

    fn mem_db() -> Db {
        Db::open(&DbConfig::in_memory()).expect("in-memory db opens")
    }

    /// [`Db::read_blocking`]/[`Db::write_blocking`] wait on a
    /// `tokio::sync::Mutex` via `blocking_lock()`, which panics if called
    /// from a task already driven by a tokio runtime (see their own doc
    /// comments). A `#[tokio::test]` body IS such a task, so every test
    /// below that needs both `run_migrations` (async) and a blocking
    /// helper moves the blocking call onto `spawn_blocking` itself, the
    /// same way `BatchWriter::barrier_async` and every real async caller
    /// must.
    #[test]
    fn in_memory_open_sets_foreign_keys_on() {
        let db = mem_db();
        let fk: u32 = db
            .read_blocking(|c| c.query_row("PRAGMA foreign_keys;", [], |r| r.get(0)))
            .expect("reads pragma");
        assert_eq!(fk, 1);
        assert_eq!(db.label(), ":memory:");
    }

    #[tokio::test]
    async fn run_migrations_then_read_write_roundtrip() {
        let db = mem_db();
        db.run_migrations(MigrationRunner::new(vec![Migration::new(
            1,
            "users",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        )]))
        .await
        .expect("migration applies");

        let write_db = db.clone();
        tokio::task::spawn_blocking(move || {
            write_db.write_blocking(|c| {
                c.execute("INSERT INTO users (id, name) VALUES (1, 'alice')", [])?;
                c.execute("INSERT INTO users (id, name) VALUES (2, 'bob')", [])?;
                Ok(())
            })
        })
        .await
        .expect("join")
        .expect("writes succeed");

        let read_db = db.clone();
        let names: Vec<String> = tokio::task::spawn_blocking(move || {
            read_db.read_blocking(|c| {
                c.prepare("SELECT name FROM users ORDER BY id")?
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect()
            })
        })
        .await
        .expect("join")
        .expect("reads succeed");
        assert_eq!(names, vec!["alice".to_owned(), "bob".to_owned()]);
    }

    #[tokio::test]
    async fn file_open_creates_parent_dir() {
        let mut path = std::env::temp_dir();
        path.push(format!("mcp-service4agent-storage-db-test-{}", std::process::id()));
        path.push("nested");
        path.push("dir");
        path.push("index.db");
        let cleanup_root = path
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
            .expect("test path has three ancestors")
            .to_path_buf();
        let _ = std::fs::remove_dir_all(&cleanup_root);

        let db = Db::open(&DbConfig::new(&path)).expect("opens, creating parent dirs");
        db.run_migrations(MigrationRunner::new(vec![Migration::new(
            1,
            "init",
            "CREATE TABLE t (n INTEGER);",
        )]))
        .await
        .expect("migration applies");
        let write_db = db.clone();
        tokio::task::spawn_blocking(move || {
            write_db.write_blocking(|c| c.execute("INSERT INTO t VALUES (42)", []).map(|_| ()))
        })
        .await
        .expect("join")
        .expect("write succeeds");

        drop(db);
        let _ = std::fs::remove_dir_all(&cleanup_root);
    }

    /// Every pragma [`WriterPragmaConfig::default`] states must actually
    /// be applied and readable back off the live connection — proves the
    /// wiring in `Db::open`, not just the config struct's own field
    /// values.
    #[tokio::test]
    async fn writer_pragmas_are_applied_and_read_back() {
        let mut path = std::env::temp_dir();
        path.push(format!("mcp-service4agent-db-writer-pragmas-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let defaults = WriterPragmaConfig::default();
        let db = Db::open(&DbConfig::new(&path)).expect("db opens");

        let read_db = db.clone();
        let (busy_timeout, cache_size, synchronous, journal_size_limit, wal_autocheckpoint): (i64, i64, i64, i64, i64) =
            tokio::task::spawn_blocking(move || {
                read_db.read_blocking(|c| {
                    Ok((
                        c.query_row("PRAGMA busy_timeout", [], |r| r.get(0))?,
                        c.query_row("PRAGMA cache_size", [], |r| r.get(0))?,
                        c.query_row("PRAGMA synchronous", [], |r| r.get(0))?,
                        c.query_row("PRAGMA journal_size_limit", [], |r| r.get(0))?,
                        c.query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))?,
                    ))
                })
            })
            .await
            .expect("join")
            .expect("read pragmas");

        assert_eq!(busy_timeout, defaults.busy_timeout_ms);
        assert_eq!(cache_size, defaults.cache_size_kib);
        assert_eq!(synchronous, 1, "NORMAL reports as 1");
        assert_eq!(journal_size_limit, defaults.journal_size_limit_bytes);
        assert_eq!(wal_autocheckpoint, defaults.wal_autocheckpoint_pages);

        drop(db);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    /// [`Db::open_with_writer_pragmas`] applies a caller-chosen budget
    /// instead of the default, proving the override path (not just that
    /// the defaults happen to work).
    #[tokio::test]
    async fn open_with_writer_pragmas_applies_a_caller_chosen_budget() {
        let mut path = std::env::temp_dir();
        path.push(format!("mcp-service4agent-db-writer-pragmas-custom-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let custom = WriterPragmaConfig {
            busy_timeout_ms: 1_234,
            cache_size_kib: -4_096,
            journal_size_limit_bytes: 16 * 1024 * 1024,
            wal_autocheckpoint_pages: 500,
        };
        let db = Db::open_with_writer_pragmas(&DbConfig::new(&path), &custom).expect("db opens with custom pragmas");

        let read_db = db.clone();
        let (busy_timeout, cache_size): (i64, i64) = tokio::task::spawn_blocking(move || {
            read_db.read_blocking(|c| {
                Ok((
                    c.query_row("PRAGMA busy_timeout", [], |r| r.get(0))?,
                    c.query_row("PRAGMA cache_size", [], |r| r.get(0))?,
                ))
            })
        })
        .await
        .expect("join")
        .expect("read pragmas");

        assert_eq!(busy_timeout, 1_234);
        assert_eq!(cache_size, -4_096);

        drop(db);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
