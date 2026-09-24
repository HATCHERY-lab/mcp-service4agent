//! Single-writer SQLite storage: one writer connection ([`Db`] +
//! [`BatchWriter`]), a bounded independent-connection read pool
//! ([`ReadPool`]), a dedicated WAL checkpoint connection ([`Checkpointer`]),
//! and versioned migrations.
//!
//! A host wires its own application state onto this the same way: every
//! read runs against [`ReadPool`], every write is enqueued on
//! [`BatchWriter`]. [`Db`] itself is never handed to a route handler — it
//! exists only to be opened once, migrated, and handed to exactly one
//! [`BatchWriter`]; every other reader goes through [`ReadPool`] instead.

pub mod batch;
pub mod checkpoint;
pub mod config;
pub mod db;
pub mod migrations;
pub mod plan_guard;
pub mod read_pool;
pub mod writer_pragmas;

pub use batch::{BatchConfig, BatchStats, BatchWriter, WriteOp, DEFAULT_SLOW_BATCH_THRESHOLD};
pub use checkpoint::Checkpointer;
pub use config::DbConfig;
pub use db::{Db, DbError};
pub use migrations::{Migration, MigrationRunner};
pub use plan_guard::{assert_uses_index, query_plan};
pub use read_pool::{
    apply_read_pragmas, per_connection_read_cache_kib, ReadKindMetric, ReadPool, ReadPoolConfig, ReadPoolMetrics,
    DEFAULT_READ_CACHE_BUDGET_KIB, DEFAULT_READ_DEADLINE, DEFAULT_READ_POOL_SIZE, DEFAULT_SLOW_QUERY_THRESHOLD,
    MMAP_SIZE_BYTES, READ_CACHE_FLOOR_KIB,
};
pub use writer_pragmas::{
    apply_writer_pragmas, WriterPragmaConfig, DEFAULT_JOURNAL_SIZE_LIMIT_BYTES, DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
    DEFAULT_WRITER_CACHE_KIB,
};
