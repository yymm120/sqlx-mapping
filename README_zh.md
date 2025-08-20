<div align="center">
  <h1>✨ sqlx-mapping ✨</h1>
  <p>PostgreSQL 到 Rust 代码生成器</p>

[![Crates.io](https://img.shields.io/crates/v/creator-sqlx?style=for-the-badge&logo=rust)](https://crates.io/crates/creator-sqlx)
[![License](https://img.shields.io/badge/license-MIT-blue?style=for-the-badge)](https://opensource.org/licenses/MIT)
[![CI](https://img.shields.io/github/actions/workflow/status/your-repo/creator-sqlx/rust.yml?style=for-the-badge&logo=github)](https://github.com/your-repo/creator-sqlx/actions)
</div>

> Only support postgres

## 🚀 功能特性

| 功能            | 描述                |
|---------------|-------------------|
| 🔄 **CRUD** | 命令行生成简单CRUD       |


## 📦 安装

```bash
cargo install sqlx-mapping
```

## 🏁 快速开始

```bash
# 1. 设置连接 (或使用 --db 参数)
export DATABASE_URL="postgresql://用户:密码@localhost:5432/数据库名"

# 2. 生成模型
cx --models

# 3. 生成CRUD (包含模型生成)
cx --crud
```

## 📜 命令参考

```text
使用方法:
    sqlx-mapping [选项] [命令]

选项:
        --crud         生成全套CRUD操作 (默认)

```


## 🎯 示例输出

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


## 📄 开源协议

MIT © [您的名字](https://github.com/your-repo)