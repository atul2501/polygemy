//! Polymarket BTC 5-minute markets: discovery (Gamma + CLOB), validation, lifecycle and the
//! manager that ties them to the CLOB collector and PostgreSQL.

pub mod discovery;
pub mod lifecycle;
pub mod manager;
pub mod repo;
pub mod validate;
