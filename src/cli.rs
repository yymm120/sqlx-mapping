use crate::codegen::{
    ModItemAttr, Options, process_ast, process_ast_update, process_gen, scan, update_mod_rs,
};
use anyhow::{Context, Result, anyhow};
use clap::Parser;
use clap::{ArgAction};
use regex::Regex;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "sm")]
#[command(about = "Generate Rust structs and CRUD functions from PostgresSQL tables", long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub db: Option<String>,

    #[arg(long, default_value = "src/models")]
    pub output: PathBuf,

    // #[arg(long, action = ArgAction::SetTrue)]
    // pub models: bool,
    //
    // #[arg(short = 'c', long, action = ArgAction::SetTrue)]
    // pub create: bool,
    //
    // #[arg(short = 'r', long, action = ArgAction::SetTrue)]
    // pub retrieve: bool,
    //
    // #[arg(short = 'u', long, action = ArgAction::SetTrue)]
    // pub update: bool,
    //
    // #[arg(short = 'd', long, action = ArgAction::SetTrue)]
    // pub delete: bool,
    #[arg(long, action = ArgAction::SetTrue)]
    pub crud: bool,
    // #[arg(long)]
    // pub config: Option<PathBuf>,
}

// const STYLES: styling::Styles = styling::Styles::styled()
//     .header(styling::AnsiColor::Green.on_default().bold())
//     .usage(styling::AnsiColor::Green.on_default().bold())
//     .literal(styling::AnsiColor::Blue.on_default().bold())
//     .placeholder(styling::AnsiColor::Cyan.on_default());

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let Cli { db, output, crud } = cli;

    // let mut cmd = Cli::command();
    // let style = cmd.color(clap::ColorChoice::Always).get_matches();

    let db_url = &db
        .or(std::env::var("DATABASE_URL").ok())
        .context("env DATABASE_URL is required! ")?;

    let pool = connect_db(db_url).await;

    if !crud {
        return Err(anyhow!("Please provide --crud option."));
    }

    let options = Options {
        mod_dir: output.clone(),
        mod_file: output.clone().join("mod.rs"),
    };

    let ((mod_file, pg_class), all_tables, mut exist_files) = scan(&options, &pool).await?;

    let mut in_file_tables = Vec::new();
    let mut in_file_old_tables = Vec::new();
    let mut rest_tables = Vec::new();

    for (table_name, table_info) in all_tables {
        if pg_class.contains_key(&table_info.oid) {
            if exist_files.contains_key(&table_name) {
                in_file_tables.push((table_info, exist_files.remove(&table_name).unwrap()));
                continue;
            } else if exist_files.contains_key(&pg_class.get(&table_info.oid).unwrap().name) {
                in_file_old_tables.push((table_info, exist_files.remove(&table_name).unwrap()));
                continue;
            } else {
                // 在mod中有定义, 但是新旧表名都没匹配上
                continue;
            }
        }
        rest_tables.push(table_info);
    }

    let mod_items = rest_tables
        .iter()
        .map(|t| ModItemAttr {
            name: t.table_name.clone(),
            table_name: t.table_name.clone(),
            oid: t.oid,
        })
        .collect::<Vec<_>>();

    tokio::join!(
        update_mod_rs(&options, pg_class, mod_file, &mod_items),
        process_ast(&options, in_file_tables),
        process_ast_update(&options, in_file_old_tables),
        process_gen(&options, rest_tables),
    );

    Ok(())
}

pub fn report_error(err: &anyhow::Error) {
    // let cmd = Cli::command();
    // let styles = cmd.styles(STYLES).get_matches();
    println!("{} {}", "ERROR: ", err);
}

pub async fn connect_db(db_url: &str) -> Pool<Postgres> {
    // let db_url = "postgres://postgres:postgres@localhost/paotui";
    PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await
        .expect("Can't connect to database")
}

#[allow(unused)]
async fn insert_or_replace_module(file_path: &Path, module_name: &str) -> Result<()> {
    let content = tokio::fs::read_to_string(file_path).await?;

    // 匹配被注释的模块声明（// 或 /* */）
    let commented_re = Regex::new(&format!(
        r"(?m)^(?P<indent>\s*)(?P<comment>//\s*|/\*\s*)(?P<rest>(pub\s+)?mod\s+{}\s*;(\s*\*/)?)",
        regex::escape(module_name)
    ))?;

    // 匹配正常模块声明
    let normal_re = Regex::new(&format!(
        r"(?m)^\s*(pub\s+)?mod\s+{}\s*;",
        regex::escape(module_name)
    ))?;

    let new_content = if let Some(caps) = commented_re.captures(&content) {
        // 取消注释：保留缩进和模块声明，只去掉注释符号
        let indent = caps.name("indent").map_or("", |m| m.as_str());
        let rest = caps.name("rest").map_or("", |m| m.as_str());
        commented_re
            .replacen(&content, 1, format!("{}{}", indent, rest))
            .to_string()
    } else if !normal_re.is_match(&content) {
        // 没有声明（无论是否注释），插入新声明
        let re_mod = Regex::new(r"(?m)^\s*(pub\s+)?mod\s+\w+\s*;")?;
        let re_use = Regex::new(r"(?m)^\s*(pub\s+)?use\s+.+;")?;
        let re_attr = Regex::new(r"(?m)^#!\[.*\]")?;

        let insertion_point = if let Some(m) = re_mod.find_iter(&content).last() {
            m.end()
        } else if let Some(m) = re_use.find_iter(&content).last() {
            m.end()
        } else if let Some(m) = re_attr.find_iter(&content).last() {
            content[m.start()..]
                .find(']')
                .map(|i| m.start() + i + 1)
                .unwrap_or(m.end())
        } else {
            0
        };

        format!(
            "{}{}\n{}",
            &content[..insertion_point],
            format!("\nmod {};", module_name),
            &content[insertion_point..]
        )
    } else {
        // 已有正常声明，不做修改
        content
    };

    tokio::fs::write(file_path, new_content).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::cli::{Cli, connect_db, insert_or_replace_module};
    use anyhow::Context;
    // use clap::error::ErrorKind;
    use clap::{CommandFactory, Parser};
    use std::path::Path;

    #[tokio::test]
    async fn test_generate_models() -> anyhow::Result<()> {
        insert_or_replace_module(Path::new("src/main.rs"), "models").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_generate_crud() -> anyhow::Result<()> {
        let args = Cli::parse_from(["cx"]);
        let mut cmd = Cli::command();
        let style = cmd.color(clap::ColorChoice::Always).get_matches();

        let db_url = &args
            .db
            .or(std::env::var("DATABASE_URL").ok())
            .context("env DATABASE_URL is required! ")?;

        let pool = connect_db(db_url).await;

        Ok(())
    }
}
