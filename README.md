<div align="center">
  <h1>✨ sqlx-mapping ✨</h1>
  <p>PostgreSQL to Rust Code Generator</p>

[![Crates.io](https://img.shields.io/crates/v/creator-sqlx?style=for-the-badge&logo=rust)](https://crates.io/crates/creator-sqlx)
[![License](https://img.shields.io/badge/license-MIT-blue?style=for-the-badge)](https://opensource.org/licenses/MIT)
[![CI](https://img.shields.io/github/actions/workflow/status/your-repo/creator-sqlx/rust.yml?style=for-the-badge&logo=github)](https://github.com/your-repo/creator-sqlx/actions)
</div>

> Only support Postgres

Unstable!
Unstable!
Unstable!

**The tool is currently unstable**

## 🚀 Features

| Feature                    | Description                          |
|----------------------------|--------------------------------------|
| ⚡ **Instant Models**       | Generate Rust structs                |
| 🔄 **Full CRUD**           | Generate Rust Simple CRUD operations |


## 📦 Installation


```bash
cargo install sqlx-mapping
```

## 🏁 Quick Start

```bash
# 1. Set connection (or use --db parameter)
export DATABASE_URL="postgresql://user:password@localhost:5432/db_name"

sqlx-mapping map --output examples/model/pg --watch

sqlx-mapping map --output examples/model/pg --watch -m

sqlx-mapping map --output examples/model/pg --watch --crud
```

## 📜 Command Reference

```text
Usage: sm map [OPTIONS]

Options:
  -w, --watch            watch mode
  -m, --model            mapping struct model
  -c, --create           mapping create sql function
  -r, --retrieve         mapping retrieve sql function
  -u, --update           mapping update sql function
  -d, --delete           mapping delete sql function
      --crud             mapping all (create/retrieve/update/delete) sql function
      --db <DB>          specify DATABASE_URL. etc.: postgres://user:password@localhost/postgres
      --output <OUTPUT>  specify where to write [default: src/model/pg]
  -h, --help             Print help
```
