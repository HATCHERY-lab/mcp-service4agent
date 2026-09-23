//! Single-writer SQLite storage: one writer connection ([`Db`] +
//! [`BatchWriter`]) and a read-only connection pool ([`DbPool`]).
//!
//! A host wires its own application state onto this the same way: every
//! read runs against [`DbPool`], every write is enqueued on [`BatchWriter`].
//! [`Db`] itself is never handed to a route handler — it exists only to be
//! opened once, migrated, and handed to exactly one [`BatchWriter`]; every
//! other reader goes through [`DbPool`] instead.

pub mod batch;
pub mod config;
pub mod db;
pub mod migrations;
pub mod pool;

pub use batch::{BatchConfig, BatchStats, BatchWriter, WriteOp};
pub use config::DbConfig;
pub use db::{Db, DbError};
pub use migrations::{Migration, MigrationRunner};
pub use pool::{DbConnection, DbPool, PoolStats};
