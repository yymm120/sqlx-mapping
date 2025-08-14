<div align="center">
  <h1>✨ Creator-Sqlx ✨</h1>
  <p>PostgreSQL to Rust Code Generator</p>

[![Crates.io](https://img.shields.io/crates/v/creator-sqlx?style=for-the-badge&logo=rust)](https://crates.io/crates/creator-sqlx)
[![License](https://img.shields.io/badge/license-MIT-blue?style=for-the-badge)](https://opensource.org/licenses/MIT)
[![CI](https://img.shields.io/github/actions/workflow/status/your-repo/creator-sqlx/rust.yml?style=for-the-badge&logo=github)](https://github.com/your-repo/creator-sqlx/actions)
</div>

> Only support Postgres

## 🚀 Features

| Feature                    | Description                          |
|----------------------------|--------------------------------------|
| ⚡ **Instant Models**       | Generate Rust structs                |
| 🔄 **Full CRUD**           | Generate Rust Simple CRUD operations |


## 📦 Installation

```bash

```

```bash
cargo install sqlx-mapping
```

## 🏁 Quick Start

```bash
# 1. Set connection (or use --db parameter)
export DATABASE_URL="postgresql://user:password@localhost:5432/db_name"

sqlx-mapping --crud
```

## 📜 Command Reference

```text
USAGE:
    cx [OPTIONS] [COMMAND]

OPTIONS:

        --crud         Generate all simple CRUD operations (default)

        --db           postgresql://user:pass@localhost:5432/db_name
```

## 🎯 Example Output

```rust
// models/user.rs
#[derive(Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub id: uuid::Uuid,
    pub username: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}
```
