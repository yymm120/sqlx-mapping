//!
//! # creator-sqlx
//!
//! Set DATABASE_URL=postgresql://username:password@localhost:5432/postgres
//! ```shell
//! # or
//! cx --crud
//! ```

use crate::cli::report_error;

pub mod cli;
pub mod codegen;
pub mod config;

#[tokio::main]
async fn main() {
    if let Err(e) = cli::run().await {
        report_error(&e);
        std::process::exit(1);
    }
}
