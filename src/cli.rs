use std::io;
use crate::codegen::{ModItemAttr, Options, process_ast, process_ast_update, process_gen, scan, update_mod_rs, create_mod_rs_if_not_exists, sqlx_mapping_init};
use anyhow::{Context, Result };
use clap::Parser;

use regex::Regex;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Error, Pool, Postgres};
use std::path::{Path, PathBuf};
use chrono::{TimeZone, Utc};
use quote::{format_ident, ToTokens};
use syn::{parse_quote, ItemStruct};

#[derive(clap::Subcommand)]
enum Command {

    /// mapping command.
    Map {
        /// watch mode.
        #[arg(short, long = "watch")]
        w: bool,

        /// mapping struct model.
        #[arg(short, long = "model")]
        m: bool,

        /// mapping create sql function.
        #[arg(short, long = "create")]
        c: bool,

        /// mapping retrieve sql function.
        #[arg(short, long = "retrieve")]
        r: bool,

        /// mapping update sql function.
        #[arg(short, long = "update" )]
        u: bool,

        /// mapping delete sql function.
        #[arg(short, long = "delete")]
        d: bool,

        /// mapping all (create/retrieve/update/delete) sql function.
        #[arg(long, )]
        crud: bool,

        /// specify DATABASE_URL. etc.: postgres://user:password@localhost/postgres
        #[arg(long)]
        db: Option<String>,


        /// specify where to write.
        #[arg(long, default_value = "examples/model/pg")]
        output: PathBuf,
    },

}

#[derive(Parser)]
#[command(name = "sm")]
#[command(about = "Generate Rust structs and CRUD functions from PostgresSQL tables", long_about = None)]
pub struct Cli {

    #[command(subcommand)]
    cmd: Option<Command>,

}

// const STYLES: styling::Styles = styling::Styles::styled()
//     .header(styling::AnsiColor::Green.on_default().bold())
//     .usage(styling::AnsiColor::Green.on_default().bold())
//     .literal(styling::AnsiColor::Blue.on_default().bold())
//     .placeholder(styling::AnsiColor::Cyan.on_default());

use chrono_tz::Asia::Shanghai;

pub async fn run() -> anyhow::Result<()> {

    let cli = Cli::parse();

    match cli.cmd {
        Some(Command::Map {w,  m, c, r, u, d, crud, db, output }) => {
            let db_url = &db
                .or(std::env::var("DATABASE_URL").ok())
                .context("env DATABASE_URL is required! ")?;
            let options = Options {
                mod_dir: output.clone(),
                mod_file: output.clone().join("mod.rs"),
                create: crud || c,
                update: crud || u,
                delete: crud || d,
                retrieve: crud || r,
                crud,
                model: crud || c || u || d || r || m,
                db_url: db_url.clone(),
            };

            let pool = connect_db(db_url).await;

            if w {
                let mut stdout = io::stdout();
                match crate::watch::run(&mut stdout, options.clone()).await {
                    Ok(status) => {
                        match status { '1' => {} _ => {} }
                    }
                    Err(e) => {
                        println!("{:#?}", e);
                        println!("{:#?}", e);
                    }
                }
            }

            if crud || m || c || r || u || d {
                let res = sqlx_mapping_init(&pool).await;
                match res {
                    Ok(_) => {}
                    Err(e) => {println!("{:#?}", e); return Ok(())}
                }


                let temp = res?;
                tokio::fs::create_dir_all(&options.mod_dir).await?;
                create_mod_rs_if_not_exists(&options.mod_file)
                  .await
                  .expect("Failed to create mod.rs");
                 let ((mod_file,mod_mappings), all_tables, mut exist_files) = scan(&options, &pool, &temp).await?;

                let mut in_file_tables = Vec::new();
                let mut in_file_old_tables = Vec::new();
                let mut rest_tables = Vec::new();

                for (table_name, table_info) in all_tables {
                    if mod_mappings.contains_key(&table_info.oid) {
                        if exist_files.contains_key(&table_info.table_name) {
                            let file_key = table_info.table_name.clone();
                            in_file_tables.push((table_info, exist_files.remove(&file_key).unwrap()));
                            continue;
                        } else if exist_files.contains_key(&table_info.old_table_name) {
                            let file_key = table_info.old_table_name.clone();
                            in_file_old_tables.push((table_info, exist_files.remove(&file_key).unwrap()));
                            continue;
                        } else {
                            // 在mod中有定义, 但是新旧表名都没匹配上
                            continue;
                        }
                    }
                    rest_tables.push(table_info);
                }

                tokio::join!(
                        update_mod_rs(&options, mod_mappings, mod_file, &rest_tables, &temp),
                        process_ast(&options, in_file_tables),
                        process_ast_update(&options, in_file_old_tables),
                        process_gen(&options, &rest_tables),
                    );

                // TODO: 冗余代码, 但可能有用
                if crud {

                } else {
                    if m {
                    }
                    if c {}
                    if r {}
                    if u {}
                    if d {}
                }
            }
        }
        None => {
            println!("no cmd passed")
        }
    }

    Ok(())
}

pub fn report_error(err: &anyhow::Error) {
    println!("{} {}", "ERROR: ", err);
}

pub async fn connect_db(db_url: &str) -> Pool<Postgres> {
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

}

