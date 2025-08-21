use anyhow::{Context, anyhow};
use std::io::{Write};
use inflector::Inflector;
use proc_macro2::{Ident, TokenStream};
use quote::{ToTokens, format_ident, quote};
use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;
use sqlx::{FromRow, PgPool};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use chrono::{TimeZone, Utc};
use chrono_tz::Asia::Shanghai;
use chrono_tz::Tz;
use syn::{Attribute, Expr, ExprLit, File, Item, ItemEnum, ItemFn, ItemMod, ItemStruct, Lit, LitStr, Meta, MetaNameValue, Type, parse_file, parse_quote, FnArg, ItemImpl, ItemUse, ItemType};
use itertools::Itertools;
use syn::parse::Parse;
use tokio::fs::OpenOptions;

#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
pub struct TableInfo {
  pub oid: u32,
  pub db_name: String,
  pub schema_name: String,
  pub table_name: String,
  #[sqlx(skip)]
  pub old_table_name: String,
  #[sqlx(skip)]
  pub columns: Vec<ColumnInfo>,
  #[sqlx(skip)]
  pub primary_keys: Vec<String>,
  #[sqlx(skip)]
  pub unique_constraints: Vec<Vec<String>>,
  #[sqlx(skip)]
  pub indexes: Vec<IndexInfo>,
  #[sqlx(skip)]
  pub foreign_key: Vec<ForeignKey>,
  #[sqlx(skip)]
  pub full_qualified_name: String,
  #[sqlx(skip)]
  pub last_modified: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnInfo {
  pub column_name: String,
  pub data_type: String,
  pub is_nullable: bool,
  pub is_identity: bool,
  pub is_generated: bool,
  pub is_default: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexInfo {
  pub index_name: String,
  pub columns: Vec<String>,
  pub is_unique: bool,
}

#[derive(Debug)]
pub struct PgClass {
  pub oid: Oid,
  pub relname: String,
}
pub async fn get_tables(pool: &PgPool, mappings: &Vec<SqlxMapping>) -> anyhow::Result<HashMap<u32, TableInfo>> {
  let mut tables_map = HashMap::<u32, TableInfo>::new();

  for table in mappings {
    if let (Ok(c), Ok(p), Ok(u), Ok(i), Ok(f)) = tokio::join!(
            get_columns(pool, &table.schema_name, &table.table_name),
            get_primary_keys(pool, &table.schema_name, &table.table_name),
            get_unique_constraints(pool, &table.table_name),
            get_indexes(pool, &table.table_name),
            get_composite_foreign_keys(pool, &table.schema_name, &table.table_name),
        ) {
      if table.table_name.starts_with("_sqlx") {
        continue;
      }
      let oid = table.oid.clone().0;
      tables_map.insert(
        table.oid.0,
        TableInfo {
          oid,
          db_name: table.database_name.clone(),
          schema_name: table.schema_name.clone(),
          table_name: table.table_name.clone(),
          old_table_name: table.old_table_name.clone(),
          columns: c,
          primary_keys: p,
          unique_constraints: u,
          indexes: i,
          foreign_key: f,
          full_qualified_name: table.full_qualified_name.clone(),
          last_modified: table.last_modified,
        },
      );
    }
  }

  Ok(tables_map)
}

async fn get_columns(
  pool: &PgPool,
  schema_name: &str,
  table_name: &str,
) -> anyhow::Result<Vec<ColumnInfo>> {
  #[derive(FromRow)]
  struct ColumnRow {
    column_name: String,
    data_type: String,
    is_nullable: String,
    is_identity: String,
    column_default: Option<String>,
    is_generated: Option<String>,
  }

  let columns = sqlx::query_as!(
        ColumnRow,
        r#"
        SELECT
            column_name as "column_name!",
            data_type as "data_type!",
            is_nullable as "is_nullable!",
            is_identity as "is_identity!",
            column_default,
            is_generated
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        "#,
        schema_name,
        table_name
    )
    .fetch_all(pool)
    .await
    .context("Failed to query columns")?;

  Ok(columns
    .into_iter()
    .map(|c| ColumnInfo {
      column_name: c.column_name,
      data_type: c.data_type,
      is_nullable: c.is_nullable == "YES",
      is_identity: c.is_identity == "YES",
      is_default: c.column_default.is_some(),
      is_generated: c.is_generated.as_deref() == Some("ALWAYS"),
    })
    .collect())
}

/// 对于复合主键 { key1, key2 }
/// 对于单一逐渐 { key1 }
async fn get_primary_keys(
  pool: &PgPool,
  schema_name: &str,
  table_name: &str,
) -> anyhow::Result<Vec<String>> {
  #[derive(FromRow)]
  struct PkRow {
    column_name: String,
  }

  let pks = sqlx::query_as!(
        PkRow,
        r#"
        SELECT
            column_name as "column_name!"
        FROM information_schema.key_column_usage
        WHERE table_name = $1
        AND constraint_name IN (
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = $1 AND table_schema = $2
            AND constraint_type = 'PRIMARY KEY'
        )
        "#,
        table_name,
        schema_name
    )
    .fetch_all(pool)
    .await
    .context("Failed to query primary keys")?;

  Ok(pks.into_iter().map(|pk| pk.column_name).collect())
}

async fn get_unique_constraints(
  pool: &PgPool,
  table_name: &str,
) -> anyhow::Result<Vec<Vec<String>>> {
  #[derive(FromRow)]
  struct ConstraintRow {
    columns: Vec<String>,
  }

  let constraints = sqlx::query_as!(
        ConstraintRow,
        r#"
        SELECT
            (
                SELECT array_agg(attname ORDER BY attnum)
                FROM (
                    SELECT
                        a.attname,
                        a.attnum
                    FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, attposition)
                    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
                    WHERE a.attrelid = c.conrelid
                ) atts
            ) as "columns!"
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON c.conrelid = t.oid
        WHERE t.relname = $1
        AND c.contype = 'u'
        AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
        ORDER BY conname
        "#,
        table_name
    )
    .fetch_all(pool)
    .await
    .context("Failed to query unique constraints")?;

  Ok(constraints.into_iter().map(|c| c.columns).collect())
}

async fn get_indexes(pool: &PgPool, table_name: &str) -> anyhow::Result<Vec<IndexInfo>> {
  #[derive(FromRow)]
  struct IndexRow {
    index_name: String,
    column_name: String,
    is_unique: bool,
  }

  let indexes = sqlx::query_as!(
        IndexRow,
        r#"
        SELECT
            i.relname as "index_name!",
            a.attname as "column_name!",
            ix.indisunique as "is_unique!"
        FROM pg_class t,
             pg_class i,
             pg_index ix,
             pg_attribute a
        WHERE t.oid = ix.indrelid
          AND i.oid = ix.indexrelid
          AND a.attrelid = t.oid
          AND a.attnum = ANY(ix.indkey)
          AND t.relkind = 'r'
          AND t.relname = $1
        ORDER BY i.relname, array_position(ix.indkey, a.attnum)
        "#,
        table_name
    )
    .fetch_all(pool)
    .await
    .context("Failed to query indexes")?;

  let mut result = Vec::new();
  let mut current_index = None;
  let mut current_columns = Vec::new();
  let mut current_is_unique = false;

  for row in indexes {
    match current_index {
      Some(ref name) if name == &row.index_name => {
        current_columns.push(row.column_name);
      }
      _ => {
        if !current_columns.is_empty() {
          result.push(IndexInfo {
            index_name: current_index.unwrap(),
            columns: current_columns,
            is_unique: current_is_unique,
          });
          current_columns = Vec::new();
        }
        current_index = Some(row.index_name);
        current_is_unique = row.is_unique;
        current_columns.push(row.column_name);
      }
    }
  }

  if !current_columns.is_empty() {
    result.push(IndexInfo {
      index_name: current_index.unwrap(),
      columns: current_columns,
      is_unique: current_is_unique,
    });
  }

  Ok(result)
}

#[derive(Debug, Deserialize)]
pub enum StructEnum {
  Stm,
  Operation,
  Create,
  Update,
  None,
}


fn get_conflict_cols(table_info: &TableInfo) -> Vec<String> {
  let mut cols = HashMap::new();
  let mut conflict_cols = Vec::new();
  for col in table_info.columns.iter() {
    *cols.entry(col.column_name.clone()).or_insert(0) += 1;
  }
  for f in table_info.foreign_key.iter() {
    for col in f.target_columns.iter() {
      *cols.entry(col.column_name.clone()).or_insert(0) += 1;
    }
  }
  for (s, count) in cols {
    if count > 1 {
      conflict_cols.push(s);
    }
  }

  conflict_cols
}

fn numbers_to_letters(n: u8) -> Vec<String> {
  if n == 0 || n > 26 {
    return vec![]; // 处理非法输入（n 必须是 1-26）
  }

  (0..n)
    .map(|i| (b'A' + i) as char)
    .map(|c| c.to_string())
    .collect::<Vec<_>>()
}

// fn generate_conflict_col_enum(table_info: &TableInfo) -> ItemEnum {
//   let fields = table_info.foreign_key.iter().map(|t| {
//     t.target_table.clone()
//   }).collect::<Vec<_>>();
//   let t_vars = numbers_to_letters(fields.len() as u8);
//   let vars = fields.iter().zip(t_vars.iter()).map(|(field, t_var)| {
//     let variant_name = Ident::new(
//       &field.to_pascal_case(),
//       proc_macro2::Span::call_site(),
//     );
//
//     let ty: Type = syn::parse_str(t_var).context("Failed parse type.").unwrap();
//
//     quote! { #variant_name(#ty) }
//   }).collect::<Vec<_>>();
//
//   parse_quote! {
//         #[derive(Debug)]
//         enum ConflictCol(#) {
//             #(#vars),*,
//         }
//   }
// }

fn generate_col_enum(table: &TableInfo) -> ItemEnum {
  // 生成枚举变体
  let variants = table.columns.iter().map(|col| {
    let variant_name = Ident::new(
      &col.column_name.to_pascal_case(),
      proc_macro2::Span::call_site(),
    );

    let field_type = parse_type(&col.data_type, false);

    quote! { #variant_name(#field_type) }
  }).collect::<Vec<_>>();

  let foreign_cols = table.foreign_key.iter().map(|col| {
    let table_name_pascal_case = &col.target_table.to_pascal_case();
    let variant_name = Ident::new(
      &format!("{}Entity", col.constraint_name.to_pascal_case()),
      proc_macro2::Span::call_site(),
    );

    let ty: Type = syn::parse_str(table_name_pascal_case).context("Failed parse type.").unwrap();
    quote! { #variant_name(#ty) }
  }).collect::<Vec<_>>();

  parse_quote! {
        #[derive(Debug)]
        enum Col {
            #(#variants),*,
            #(#foreign_cols),*
        }
    }
}

fn generate_use_deps(options: &Options, table: &TableInfo) -> Vec<ItemUse> {
  let Options { mod_dir, mod_file, create, update, delete, retrieve, crud, model, .. } = options;
  let p = mod_dir.display().to_string();
  table.foreign_key
    .iter()
    .unique_by(|t| t.target_table.clone())
    .map(|t| {
      let table_name = &t.target_table;
      let struct_name = &t.target_table.to_pascal_case();
      let table_name_snake = table_name.to_snake_case();
      let table_name_ident = Ident::new(&table_name_snake, proc_macro2::Span::call_site());

      let struct_name_ident = if *model {Ident::new(struct_name, proc_macro2::Span::call_site()).to_token_stream()} else {TokenStream::new()};

      let mut extend_deps = TokenStream::new();
      if *create && *update {
        let create_ident = Ident::new(&format!("{}ForCreate", struct_name), proc_macro2::Span::call_site()).to_token_stream();
        let update_ident = Ident::new(&format!("{}ForUpdate", struct_name), proc_macro2::Span::call_site()).to_token_stream();
        extend_deps = parse_quote! {
          #table_name_ident::{#struct_name_ident, #create_ident, #update_ident}
        }
      } else if *create {
        let create_ident = Ident::new(&format!("{}ForCreate", struct_name), proc_macro2::Span::call_site()).to_token_stream();
        extend_deps = parse_quote! {
          #table_name_ident::{#struct_name_ident, #create_ident}
        }
      } else if *update {
        let update_ident = Ident::new(&format!("{}ForUpdate", struct_name), proc_macro2::Span::call_site()).to_token_stream();
        extend_deps = parse_quote! {
          #table_name_ident::{#struct_name_ident, #update_ident}
        }
      }

      parse_quote! {
                use crate::model::pg::{#table_name_ident, #extend_deps};
            }
    })
    .collect()
}
fn parse_type(sql_type: &str, is_nullable: bool) -> Type {
  let type_str = map_type(sql_type, is_nullable);
  syn::parse_str(&type_str).expect(&format!("Failed to parse the type: {}", sql_type))
}
pub fn map_type(sql_type: &str, is_nullable: bool) -> String {
  let rust_type = match sql_type.to_lowercase().as_str() {
    "bool" | "boolean" => "bool",
    "char" | "character" => "i8",
    "smallint" | "int2" | "smallserial" => "i16",
    "integer" | "int" | "int4" | "serial" => "i32",
    "bigint" | "int8" | "bigserial" => "i64",
    "real" | "float4" => "f32",
    "double precision" | "float8" => "f64",
    "varchar" | "character varying" | "text" | "bpchar" | "name" | "citext" => {
      "String"
    }
    "bytea" => "Vec<u8>",
    "numeric" | "decimal" => "bigdecimal::BigDecimal",
    "date" => "chrono::NaiveDate",
    "time" | "time without time zone" => "chrono::NaiveTime",
    "timestamp" | "timestamp without time zone" => "chrono::NaiveDateTime",
    "timestamp with time zone" | "timestamptz" => "chrono::DateTime<chrono::Utc>",
    "interval" => "sqlx::postgres::PgInterval",
    "money" => "sqlx::postgres::PgMoney",
    "uuid" => "uuid::Uuid",
    "json" | "jsonb" => "serde_json::Value",
    "inet" | "cidr" => "ipnetwork::IpNetwork",
    "macaddr" => "mac_address::MacAddress",
    "bit" | "varbit" => "bit_vec::BitVec",
    "ltree" => "sqlx::postgres::PgLTree",
    "lquery" => "sqlx::postgres::PgLQuery",
    "cube" => "sqlx::postgres::PgCube",
    "point" => "sqlx::postgres::PgPoint",
    "line" => "sqlx::postgres::PgLine",
    "lseg" => "sqlx::postgres::PgLSeg",
    "box" => "sqlx::postgres::PgBox",
    "path" => "sqlx::postgres::PgPath",
    "polygon" => "sqlx::postgres::PgPolygon",
    "circle" => "sqlx::postgres::PgCircle",
    "hstore" => "sqlx::postgres::PgHstore",
    "timetz" => "sqlx::postgres::PgTimeTz",
    _ => return "".into(),
  };

  if is_nullable {
    format!("Option<{}>", rust_type)
  } else {
    rust_type.to_string()
  }
}

pub fn generate_struct(table: &TableInfo, struct_type: &StructEnum) -> ItemStruct {
  let struct_name = match struct_type {
    StructEnum::Create => {
      format_ident!("{}{}", table.table_name.to_pascal_case(), "ForCreate")
    }
    StructEnum::Update => {
      format_ident!("{}{}", table.table_name.to_pascal_case(), "ForUpdate")
    }
    StructEnum::Stm => {
      format_ident!("{}", table.table_name.to_pascal_case())
    }
    _ => {
      format_ident!("{}", table.table_name.to_pascal_case())
    }
  };

  let derives = match &struct_type {
    StructEnum::Stm => {
      quote! {#[derive(Debug, FromRow, Serialize, Deserialize, Clone, Default)]}
    }
    StructEnum::Create => {
      quote! {#[derive(Debug, Serialize, Deserialize, Clone, Default)]}
    }
    StructEnum::Update => {
      quote! {#[derive(Debug, Serialize, Deserialize, Clone, Default)]}
    }
    _ => {
      quote! {}
    }
  };

  let mut fields = table
    .columns
    .iter()
    .filter(|col| match struct_type {
      StructEnum::Stm => true,
      StructEnum::Update => !col.is_identity,
      StructEnum::Create => !col.is_generated && !col.is_identity,
      _ => false,
    })
    .map(|column| {
      let field_name = format_ident!("{}", column.column_name.to_snake_case());
      let field_type = match struct_type {
        StructEnum::Stm => parse_type(&column.data_type, column.is_nullable),
        StructEnum::Create => {
          if column.is_default {
            parse_type(&column.data_type, true)
          } else {
            parse_type(&column.data_type, column.is_nullable)
          }
        }
        StructEnum::Update => {
          if table.primary_keys.iter().any(|k| k == &column.column_name) {
            parse_type(&column.data_type, false)
          } else {
            parse_type(&column.data_type, true)
          }
        }
        _ => return Err(anyhow!("occur error!")),
      };
      Ok(quote! {
                #field_name: #field_type
            })
    })
    .collect::<anyhow::Result<Vec<_>>>()
    .unwrap_or_default();

  // let mut foreign_fields: Vec<TokenStream> = Vec::new();
  // match struct_type {
  //   StructEnum::Stm => {
  //     foreign_fields = table.foreign_key.iter().map(|col| {
  //       let table_name_pascal_case = &col.target_table.to_pascal_case();
  //       let variant_name = Ident::new(
  //         &format!("{}", col.constraint_name.to_snake_case()),
  //         proc_macro2::Span::call_site(),
  //       );
  //       let ty: Type = syn::parse_str(table_name_pascal_case).context("Failed parse type.").unwrap();
  //       quote! {
  //     #[sqlx(skip)]
  //     #variant_name: #ty
  //   }
  //     }).collect::<Vec<_>>();
  //   }
  //   _ => {}
  // }
  //
  // fields.append(&mut foreign_fields);


  // #[doc = #oid_comment]
  let comment = match struct_type {
    StructEnum::Stm => {
      format!(" {}, {} ", &table.table_name, &table.oid)
    }
    _ => {
      "".to_string()
    }
  };


  let output = parse_quote! {
        #[doc = #comment]
        #derives
        pub struct #struct_name {
            #(
                #fields,
            )*
        }
    };
  output
}

pub fn generate_where_struct(table: &TableInfo) -> ItemStruct {
  parse_quote! {
        #[derive(Debug)]
        pub struct Where(Vec<Col>);
    }
}

pub fn generate_main_struct_expand(table: &TableInfo) -> ItemStruct {
  parse_quote! {
        #[derive(Debug)]
        pub struct Where(Vec<Col>);
    }
}

pub fn generate_where_imp(table: &TableInfo) -> ItemImpl {
  let match_arms: Vec<syn::Arm> = table.columns.iter().enumerate().map(|(_, col)| {
    let col_name = &col.column_name;
    let variant_name = syn::Ident::new(&col_name.to_pascal_case(), proc_macro2::Span::call_site());

    let format_str = format!("{} = ${{}}", col_name);

    parse_quote! {
            Col::#variant_name(_) => format!(#format_str, i + 1),
        }
  }).collect();

  // let cols = table.columns.iter().map(|col| {
  //     &col.column_name
  // }).collect::<Vec<_>>();

  parse_quote! {
        impl Where {
            pub fn new(cols: Vec<Col>) -> Self {
                Self(cols)
            }

            pub fn to_clause(&self) -> String {
                self.0.iter()
                    .enumerate()
                    .map(|(i, col)| match col {
                        #(#match_arms)*
                        _ => unreachable!("Unexpected column type"),
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ")
            }
        }
    }
}

enum SafeOp {
  AcceptNone, // 变更为None
  IgnoreNone, // 不理会None
}

fn generate_update_function(table: &TableInfo, safe_op: SafeOp) -> anyhow::Result<ItemFn> {
  let struct_name = format_ident!("{}{}", table.table_name.to_pascal_case(), "ForUpdate");
  let method_name = match safe_op {
    SafeOp::AcceptNone => {
      format_ident! {"{}", "update"}
    }
    SafeOp::IgnoreNone => {
      format_ident! {"{}", "update_safe"}
    }
  };
  let return_type = format_ident!("{}", table.table_name.to_pascal_case());
  // 获取需要更新的列（非主键且非生成列）
  let set_columns: Vec<_> = table
    .columns
    .iter()
    .filter(|c| !c.is_generated && !table.primary_keys.contains(&c.column_name))
    // .filter(|c| !c.is_generated )
    .map(|c| format_ident!("{}", c.column_name))
    .collect();

  // 如果没有可更新的列，应该返回错误或生成不同的函数
  if set_columns.is_empty() {
    let msg = format!(
      "Table {} has no updatable columns (all columns are primary keys or generated)",
      table.table_name
    );
    return Err(anyhow!("{}", msg));
  }

  let set_values = set_columns.iter().map(|col| quote! { data.#col });

  let where_values = table
    .primary_keys
    .iter()
    .map(|pk| format_ident!("{}", pk))
    .map(|pk| quote! { data.#pk });

  // 构建 SET 子句
  let set_clause = set_columns
    .iter()
    .enumerate()
    .map(|(i, col)| match safe_op {
      SafeOp::AcceptNone => {
        format!("{} = ${}", col, i + 1)
      }
      SafeOp::IgnoreNone => {
        format!("{} = COALESCE(${}, {})", col, i + 1, col)
      }
    })
    .collect::<Vec<_>>()
    .join(",\n  ");

  // 构建 WHERE 子句
  let where_clause = table
    .primary_keys
    .iter()
    .enumerate()
    .map(|(i, pk)| format!("{} = ${}", pk, set_columns.len() + i + 1))
    .collect::<Vec<_>>()
    .join(" AND ");

  // 构建 RETURNING 子句
  let returning = table
    .columns
    .iter()
    .map(|c| c.column_name.to_string())
    .collect::<Vec<_>>()
    .join(",");

  let query = format!(
    "UPDATE {}.{}.{}\nSET\n  {}\nWHERE {}\nRETURNING {}\n",
    table.db_name, table.schema_name, table.table_name, set_clause, where_clause, returning
  );

  // 拆分成多行，并手动拼接成带换行的字符串
  let query = query.replace('\n', "\n         "); // 添加缩进
  let query = syn::parse_str::<syn::LitStr>(&format!(r#""{}""#, query)).unwrap();
  let query = quote! { #query };

  Ok(parse_quote! {
        pub async fn #method_name<'a, E>(executor: E, data: &#struct_name) -> Result<#return_type, sqlx::Error>
        where
            E: sqlx::Executor<'a, Database = sqlx::Postgres>,
        {
          // language=postgresql
            sqlx::query_as::<_, #return_type>(#query)
            #(.bind(&#set_values))*
            #(.bind(&#where_values))*
            .fetch_one(executor)
            .await
        }
    })
}

fn generate_insert_function(table: &TableInfo) -> ItemFn {
  let table_name = &table.table_name;
  let struct_name = format_ident!("{}ForCreate", table_name.to_pascal_case());
  let return_type = format_ident!("{}", table_name.to_pascal_case());

  // 生成字段处理逻辑
  let mut field_handlers = Vec::new();
  for (_, col) in table.columns.iter().enumerate() {
    let col_name = format_ident!("{}", col.column_name);
    let col_enum = format_ident!("{}", col.column_name.to_pascal_case());

    if col.is_generated {
      continue
    }

    let handler = if col.is_nullable || col.is_default {
      quote! {
                match &data.#col_name {
                    None => {}
                    Some(val) => {
                        cols.push(Col::#col_enum(val.clone()));
                        column_names.push(stringify!(#col_name));
                        placeholders.push(format!("${}", column_names.len()));
                    }
                }
            }
    } else {
      quote! {
                cols.push(Col::#col_enum(data.#col_name.clone()));
                column_names.push(stringify!(#col_name));
                placeholders.push(format!("${}", column_names.len()));
            }
    };

    field_handlers.push(handler);
  }

  // 生成返回字段
  let return_columns = table
    .columns
    .iter()
    .map(|col| format_ident!("{}", col.column_name))
    .collect::<Vec<_>>();

  let enum_columns = table
    .columns
    .iter()
    .filter(|col | !col.is_generated)
    .map(|col| format_ident!("{}", col.column_name.to_pascal_case()))
    .collect::<Vec<_>>();

  let table_full_name = format!(
    "{}.{}.{}",
    table.db_name, table.schema_name, table.table_name
  );
  let table_full_name =
    syn::parse_str::<syn::LitStr>(&format!(r#""{}""#, table_full_name)).unwrap();

  let params: Vec<TokenStream> = if table.foreign_key.len() > 0 {
     table
      .foreign_key
      .iter()
      .map(|fk| {
        let param_name = format_ident!("{}", fk.constraint_name.to_snake_case());
        let ty: Type = syn::parse_str(&format!("&Option<&{}ForCreate>", fk.target_table.to_pascal_case()))
          .unwrap_or_else(|_| panic!("Failed to parse type: {}", fk.target_table));
        parse_quote! { #param_name: #ty }
      })
      .collect()
  } else {
    vec! {TokenStream::new()}
  };

  let f_querys: Vec<TokenStream> = if table.foreign_key.len() > 0 {
    table
      .foreign_key
      .iter()
      .map(|fk| {
        let model_name = format_ident!("{}", fk.target_table.to_snake_case());
        let param_name = format_ident!("{}", fk.constraint_name.to_snake_case());
        let ty: Type = syn::parse_str(&format!("&Option<&{}ForCreate>", fk.target_table.to_pascal_case()))
          .unwrap_or_else(|_| panic!("Failed to parse type: {}", fk.target_table));
        parse_quote! {
          match #param_name {
            Some(data) => {#model_name::insert(executor, data).await?;}
            None => {}
          }
        }
      })
      .collect()
  } else {
    vec! {TokenStream::new()}
  };



  parse_quote! {
        pub async fn insert<'a, E>(executor: E, data: &#struct_name, #(#params),*) -> Result<#return_type, sqlx::Error>
        where
            E: sqlx::Executor<'a, Database = sqlx::Postgres> + Copy,
        {

            #(#f_querys;)*
            let mut cols: Vec<Col> = Vec::new();
            let mut column_names = Vec::new();
            let mut placeholders = Vec::new();

            #(#field_handlers);*

            let sql = format!(
                r#"INSERT INTO {} ( {} ) VALUES ( {} ) RETURNING {} "#,
                #table_full_name,
                column_names.join(", "),
                placeholders.join(", "),
                stringify!(#(#return_columns),*)
            );

            let mut query = sqlx::query_as(&sql);

            for col in cols {
                match col {
                    #(
                        Col::#enum_columns(val) => { query = query.bind(val); }
                    )*
                    _ => {}
                }
            }

            query.fetch_one(executor).await
        }
    }
}

fn generate_insert_cascade_function(table: &TableInfo) -> ItemFn {
  let table_name = &table.table_name;
  let struct_name = format_ident!("{}ForCreate", table_name.to_pascal_case());
  let return_type = format_ident!("{}", table_name.to_pascal_case());

  // 生成函数参数
  let params: Vec<syn::FnArg> = table
    .foreign_key
    .iter()
    .map(|fk| {
      let param_name = format_ident!("{}", fk.constraint_name.to_snake_case());
      let ty: Type = syn::parse_str(&format!("{}ForCreate", fk.target_table.to_pascal_case()))
        .unwrap_or_else(|_| panic!("Failed to parse type: {}", fk.target_table));
      parse_quote! { #param_name: #ty }
    })
    .collect();

  let foreign_key_inserts = table.foreign_key.iter().map(|fk| {
    let param_name = format_ident!("{}", fk.constraint_name.to_snake_case());
    let field_name = format_ident!("{}", fk.constraint_name.to_snake_case());
    let target_table = format_ident!("{}", fk.target_table.to_snake_case());

    parse_quote! {
            let #field_name = crate::model::pg::#target_table::insert(executor, &#param_name).await?;
        }
  }).collect::<Vec<syn::Stmt>>();

  let foreign_key_assignments = table.foreign_key.iter().map(|fk| {
    let field_name = format_ident!("{}", fk.constraint_name.to_snake_case());
    parse_quote! {
            #field_name: Some(#field_name),
        }
  }).collect::<Vec<syn::Stmt>>();

  parse_quote! {
        pub async fn insert_cascade<'a, E>(
            executor: E,
            data: &#struct_name,
            #(#params),*
        ) -> Result<#return_type, sqlx::Error>
        where
            E: sqlx::Executor<'a, Database = sqlx::Postgres>,
        {
            #(#foreign_key_inserts)*

            let mut result = crate::model::pg::#table_name::insert(executor, data).await?;

            #(#foreign_key_assignments)*

            Ok(result)
        }
    }
}

fn generate_delete_function(table: &TableInfo) -> ItemFn {
  let table_name = &table.table_name;
  let pk_args = table
    .columns
    .iter()
    .filter(|col| table.primary_keys.contains(&col.column_name))
    .map(|pk| format!("{}: &{}", pk.column_name, map_type(&pk.data_type, false)))
    .collect::<Vec<_>>()
    .join(", ")
    .parse::<TokenStream>()
    .expect("");

  let where_clause = table
    .primary_keys
    .iter()
    .enumerate()
    .map(|(i, pk)| format!("{} = ${}", pk, i + 1))
    .collect::<Vec<_>>()
    .join(" AND ");

  let where_values = table
    .primary_keys
    .iter()
    .map(|pk| format_ident!("{}", pk))
    .map(|pk| quote! { #pk });

  let query_content = format!(
    "DELETE FROM {}.{}.{} WHERE {}",
    table.db_name, table.schema_name, table_name, where_clause
  );

  parse_quote! {
        pub async fn delete<'a, E>(executor: E, #pk_args) -> Result<(), sqlx::Error>
        where
            E: sqlx::Executor<'a, Database = sqlx::Postgres>,
        {
            sqlx::query!(
                #query_content,
                #(#where_values),*)
                .execute(executor)
                .await?;
            Ok(())
        }
    }
}

fn generate_retrieve_functions(table: &TableInfo) -> anyhow::Result<Vec<ItemFn>> {
  let struct_name = format_ident!("{}", table.table_name.to_pascal_case());
  let mut functions = Vec::new();

  // Primary key query
  if !table.primary_keys.is_empty() {
    functions.push(generate_primary_key_query(table, &struct_name)?);
  }

  // Unique constraints
  for constraint in &table.unique_constraints {
    functions.push(generate_constraint_query(
      table,
      &struct_name,
      constraint,
      true,
    )?);
  }

  // Indexes
  for index in &table.indexes {
    if !index.is_unique || index.columns.len() > 1 {
      functions.push(generate_constraint_query(
        table,
        &struct_name,
        &index.columns,
        false,
      )?);
    }
  }

  // join 查询
  // functions.append(&mut generate_join_query(table)?);
  Ok(functions)
}

// fn generate_join_query(table: &TableInfo) -> anyhow::Result<Vec<ItemFn>> {
//
//   for k in table.foreign_key.clone().into_iter(){
//     let return_type = format_ident!("{}Extend", table.table_name);
//     let fn_name = format_ident!("query_join_{}", k.target_table);
//     let params: Vec<syn::FnArg> = k.source_keys
//       .iter()
//       .map(|fk| {
//         let param_name = format_ident!("{}", fk.to_snake_case());
//         let param_type = find_column_type(table, fk);
//         Ok(parse_quote! { #param_name: #param_type })
//       })
//       .collect::<anyhow::Result<Vec<_>>>()?;
//
//     let main_cols_ident = table.columns.iter().map(|col| {
//       format_ident!("{}", col.column_name.to_snake_case())
//     }).collect::<Vec<_>>();
//
//       let main_cols_str = table.columns.iter().map(|col| {
//           col.column_name.to_snake_case()
//       }).collect::<Vec<_>>();
//
//       let join_tables = table.foreign_key.iter().map(|col|  {
//           col.target_table.clone()
//       }).collect::<Vec<_>>();
//
//       let cols = table.foreign_key.iter().flat_map(|fk| {
//           fk.target_columns.iter().map(|col| {col.column_name.clone()})
//       }).collect::<Vec<_>>();
//
//       let left_join = table.foreign_key.iter().map(|fk| {
//           let sn = &table.table_name;
//           let tn = &fk.target_table;
//           let join_on = fk.source_keys.iter().zip(fk.target_keys.iter()) .map(|(sk, tk)| {
//              format!("{sn}.{sk} = {tn}.{tk}")
//           }).collect::<Vec<_>>().join(" AND ");
//           format!("LEFT JOIN {tn} ON {join_on}")
//       }).collect::<Vec<_>>().join("\n");
//
//       let query_str = format!(r#"
//       SELECT
//       {},
//       {}
//       FROM {}
//       {}
//       "#, main_cols_str.join(", "), cols.join(", "), table.table_name, left_join);
//
//
//     let fn_item: ItemFn = parse_quote! {
//         pub async fn #fn_name<'a, E>(executor: E, where_s: Where) -> Result<#return_type, sqlx::Error>
//         where
//             E: sqlx::Executor<'a, Database = sqlx::Postgres> {
//
//             let where_clause = where_c.to_clause();
//
//             format!("WHERE {}", &where_clause);
//
//             sqlx::query_as!(#return_type,
//                 #query_str,
//                 #(#args),*
//             )
//             .fetch_one(executor)
//             .await
//         }
//     };
//   };
//
//   let fn_name = format_ident!("query_join{}", table.table_name.to_pascal_case());
//
//   Ok(parse_quote! {
//     pub async fn #target_table<'a, E>(executor: E, #(#params),*) -> Result<#struct_name, sqlx::Error>
//         where
//             E: sqlx::Executor<'a, Database = sqlx::Postgres>,
//         {
//             sqlx::query_as!(#struct_name,
//                 #query_str,
//                 #(#args),*
//             )
//             .fetch_one(executor)
//             .await
//         }
//     })
// }

fn generate_primary_key_query(table: &TableInfo, struct_name: &Ident) -> anyhow::Result<ItemFn> {
  // let table_name = &table.table_name;
  let params: Vec<syn::FnArg> = table
    .primary_keys
    .iter()
    .map(|pk| {
      let param_name = format_ident!("{}", pk.to_snake_case());
      let param_type = find_column_type(table, pk);
      Ok(parse_quote! { #param_name: #param_type })
    })
    .collect::<anyhow::Result<Vec<_>>>()?;

  let where_clause = table
    .primary_keys
    .iter()
    .enumerate()
    .map(|(i, pk)| format!("{} = ${}", pk, i + 1))
    .collect::<Vec<_>>()
    .join(" AND ");

  let args = table
    .primary_keys
    .iter()
    .map(|pk| format_ident!("{}", pk.to_snake_case()));

  let full_table_name = format!(
    "{}.{}.{}",
    table.db_name, table.schema_name, table.table_name
  );

  let query_str = format!(
    "SELECT * FROM {} WHERE {}",
    full_table_name.to_string(),
    where_clause.to_string()
  );
  Ok(parse_quote! {
    pub async fn query_by_primary_key<'a, E>(executor: E, #(#params),*) -> Result<#struct_name, sqlx::Error>
        where
            E: sqlx::Executor<'a, Database = sqlx::Postgres>,
        {
          // language=postgresql
            sqlx::query_as::<_, #struct_name>(#query_str)
            #(.bind(&#args))*
            .fetch_one(executor)
            .await
        }
    })
}

fn generate_constraint_query(
  table: &TableInfo,
  struct_name: &Ident,
  columns: &[String],
  is_unique: bool,
) -> anyhow::Result<ItemFn> {
  let method_name = format_ident!("query_by_{}", columns.join("_").to_snake_case());
  // let table_name = &table.table_name;

  let params: Vec<syn::FnArg> = columns
    .iter()
    .map(|col| {
      let param_name = format_ident!("{}", col.to_snake_case());
      let param_type = find_column_type(table, col);
      Ok(parse_quote! { #param_name: #param_type })
    })
    .collect::<anyhow::Result<Vec<_>>>()?;

  let where_clause = columns
    .iter()
    .enumerate()
    .map(|(i, col)| format!("{} = ${}", col, i + 1))
    .collect::<Vec<_>>()
    .join(" AND ");

  let args = columns
    .iter()
    .map(|col| format_ident!("{}", col.to_snake_case()));

  let return_type: Box<syn::Type> = if is_unique {
    parse_quote! { #struct_name }
  } else {
    parse_quote! { Vec<#struct_name> }
  };

  let fetch_method = if is_unique {
    quote! { .fetch_one(executor) }
  } else {
    quote! { .fetch_all(executor) }
  };

  let query_content = format!(
    "SELECT * FROM {}.{}.{} WHERE {}",
    table.db_name, table.schema_name, table.table_name, where_clause
  );

  Ok(parse_quote! {
     pub async fn #method_name<'a, E>(executor: E, #(#params),*) -> Result<#return_type, sqlx::Error>
        where
            E: sqlx::Executor<'a, Database = sqlx::Postgres>,
        {
          // language=postgresql
            sqlx::query_as::<_, #struct_name>(#query_content)
              #(.bind(#args))*
              #fetch_method
              .await
        }
    })
}

#[derive(Debug)]
pub struct ParsedFile {
  // pub oid: i32,
  pub name: String,
  pub path: PathBuf,
  pub content: String,
  pub ast: File,
}

fn find_column_type(table: &TableInfo, column_name: &str) -> Type {
  table
    .columns
    .iter()
    .find(|c| c.column_name == column_name)
    .map(|c| parse_type(&c.data_type, c.is_nullable))
    .unwrap_or(syn::parse_str("String").unwrap())
}

pub async fn process_ast(options: &Options, mut tables: Vec<(TableInfo, ParsedFile)>) {
  let gen_tasks = tables
    .drain(..)
    .into_iter()
    .filter_map(move |(table, file)| Some(update_model_ast(options, table, file)))
    .collect::<Vec<_>>();
  let res = tokio::try_join!(futures::future::try_join_all(gen_tasks));
  match res {
    Ok(_) => {}
    Err(e) => {
      println!("generate error: {:#?}", e);
    }
  }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ModItemAttr {
  pub name: String,
  pub table_name: String,
  pub oid: u32,
  pub time: chrono::DateTime<Utc>,
}

pub async fn create_mod_rs_if_not_exists(mod_path: &PathBuf) -> anyhow::Result<()> {
  if tokio::fs::try_exists(mod_path).await? {
    Ok(())
  } else {
    tokio::fs::write(mod_path, "")
      .await
      .context("Failed to write mod.rs")
  }
}

pub async fn scan_mod_rs(
  mod_path: &PathBuf,
  pool: &PgPool,
  mappings: &Vec<SqlxMapping>
) -> (File, HashMap<u32, SqlxMapping>) {

  let mod_content = tokio::fs::read_to_string(mod_path)
    .await
    .expect("Failed to read mod.rs");
  let mod_file = syn::parse_file(&mod_content).expect("");
  let mod_items = mod_file
    .items
    .iter()
    .map(|mod_item| {
      match mod_item {
        Item::Mod(item) => {
          for attr in item.attrs.iter() {
            if let Attribute {
              meta:
              Meta::NameValue(MetaNameValue {
                                value:
                                Expr::Lit(ExprLit {
                                            lit: Lit::Str(str), ..
                                          }),
                                ..
                              }),
              ..
            } = attr
            {
              let info = (&str.to_token_stream().to_string())
                .replace(' ', "")
                .replace('\\', "")
                .clone();
              let split_v = info[1..&info.len() - 1].split(",").collect::<Vec<_>>();
              let oid = split_v[1].parse::<u32>();
              let time = split_v.get(2);
              let time = match time {
                None => {continue}
                Some(t) => {
                  let naive_dt = chrono::NaiveDateTime::parse_from_str(
                    t.trim_end_matches("_CST"),
                    "%Y-%m-%d_%H:%M:%S%.f"
                  );
                  match naive_dt {
                    Ok(t) => {
                      Shanghai.from_local_datetime(&t).unwrap()
                    }
                    Err(e) => {println!("parse err: {:#?}, input: {}", e, t); continue}
                  }
                }
              };

              match oid {
                Ok(oid) => {
                  let temp = mappings.iter().find(|p| p.oid.0 == oid);
                  match temp {
                    None => {}
                    Some(mapping) => {
                      let mut res = mapping.clone();
                      res.old_table_name = split_v[0].to_string();
                      res.last_modified = time.with_timezone(&Utc);
                      return Some(res);
                    }
                  }
                }
                Err(e) => {
                  println!("generate error: {:#?}", e);
                }
              }
            }
          }
          None
        }
        _ => None,
      }
    })
    .filter_map(|item| {
      match item {
        None => {None}
        Some(item) => {Some((item.oid.0, item))}
      }
    })
    .collect::<HashMap<u32, SqlxMapping>>();

  (mod_file, mod_items)
}

pub async fn scan(
  options: &Options,
  pool: &PgPool,
  mappings: &Vec<SqlxMapping>
) -> anyhow::Result<(
  (File, HashMap<u32, SqlxMapping>),
  HashMap<u32, TableInfo>,
  HashMap<String, ParsedFile>,
)> {
  let Options { mod_dir, mod_file, .. } = options;
  let (info_from_mod, info_from_db, info_from_file) = tokio::join!(
        scan_mod_rs(mod_file, &pool, mappings),
        get_tables(&pool, mappings),
        read_all_files_async(mod_dir)
    );
    let mut tables = info_from_db?;

    merge_info(&info_from_mod.1, &mut tables);
    Ok((info_from_mod, tables, info_from_file?))
}


fn merge_info(mappings: &HashMap<u32, SqlxMapping>, tables: &mut HashMap<u32, TableInfo>) {
  for (key, table) in tables {
    match mappings.get(key) {
      None => {}
      Some(map) => {
        if map.last_modified < table.last_modified {
          table.last_modified = map.last_modified;
        }
        // table.table_name = map.old_table_name.clone();
      }
    }
  }
}

pub async fn update_model_ast(
  options: &Options,
  table_info: TableInfo,
  mut file: ParsedFile,
) -> anyhow::Result<()> {
  let Options { mod_dir, create, update, delete, retrieve, crud, model, .. } = options;
  let table_info = &table_info;
  let _ = gen_items_if_not_exist(&mut file.ast, options, table_info);
  for item in &mut file.ast.items {
    match item {
      Item::Enum(item_enum) => {
        if !create {
          continue
        }
        let enum_name = item_enum.ident.to_token_stream().to_string();
        if enum_name == format!("{}Col", file.name.to_pascal_case()) {
          // 字段长度/名称/类型变更
          *item = Item::Enum(generate_col_enum(table_info));
          continue;
        }
      }
      Item::Fn(item_fn) => {
        let method_name = item_fn.sig.ident.to_token_stream().to_string();
        if *create && method_name.to_snake_case() == "insert" {
          *item = Item::Fn(generate_insert_function(table_info));
          continue;
        } else if *delete && method_name.to_snake_case() == "delete" {
          *item = Item::Fn(generate_delete_function(table_info));
          continue;
        } else if *update && method_name.to_snake_case() == "update" {
          match generate_update_function(table_info, SafeOp::AcceptNone) {
            Ok(update_fn) => {
              *item = Item::Fn(update_fn);
            }
            Err(_) => {}
          }
          continue;
        } else if *update && method_name.to_snake_case() == "update_safe" {
          match generate_update_function(table_info, SafeOp::IgnoreNone) {
            Ok(update_fn) => {
              *item = Item::Fn(update_fn);
            }
            Err(_) => {}
          }
          continue;
        } else if *retrieve && method_name.to_snake_case().starts_with("retrieve") {
          // *item = Item::Struct(generate_struct(table_info, &StructEnum::Update));
          continue;
        }
      }
      Item::Struct(item_struct) => {
        let struct_name = item_struct.ident.to_token_stream().to_string();
        if *model && struct_name.to_snake_case() == file.name {
          *item = Item::Struct(generate_struct(table_info, &StructEnum::Stm));
          continue;
        } else if *create && struct_name.ends_with("ForCreate") {
          *item = Item::Struct(generate_struct(table_info, &StructEnum::Create));
          continue;
        } else if *update && struct_name.ends_with("ForUpdate") {
          *item = Item::Struct(generate_struct(table_info, &StructEnum::Update));
          continue;
        } else if *create && struct_name.ends_with("Expand") && table_info.foreign_key.len() > 0 {
          *item = Item::Struct(generate_expand_struct(table_info))
        } else if *model && item_struct.attrs.iter().find(|attr| {
          attr.to_token_stream().to_string().contains( "FromRow");
          let Attribute { meta: Meta::NameValue(MetaNameValue { value: Expr::Lit(ExprLit { lit: Lit::Str(str), ..}), ..}), ..} = attr else {
            return false
          };
          str.to_token_stream().to_string().contains(&table_info.oid.to_string())
        }).is_some() {
          *item = Item::Struct(generate_struct(table_info, &StructEnum::Stm));
          continue;
        }
      }
      Item::Use(_) => {}
      _ => {}
    }
  }
  let path = mod_dir.join(format!("{}.rs", file.name));
  tokio::fs::write(path, prettyplease::unparse(&file.ast)).await?;
  Ok(())
}

fn gen_items_if_not_exist(file: &mut File, options: &Options, table_info: &TableInfo) -> HashMap<String, String>{
  let mut last_gen = Vec::new();
  for attr in file.attrs.iter() {
    if let Meta::NameValue(MetaNameValue {value: Expr::Lit(ExprLit {lit: Lit::Str(str), ..}), ..}) = &attr.meta {
      let token = str.to_token_stream().to_string().replace("\"", "").split("\n").map(|t| t.to_string()).collect::<Vec<_>>();
      last_gen = token[0].replace("include:", "").replace(" ", "").split(",").into_iter().map(|t| t.to_string()).collect::<Vec<String>>()
    }
  }
  let Options { mod_dir, mod_file, create, update, delete, retrieve, crud, model, db_url } = options;
  let mut op = HashMap::new();

  if *model {

    if !last_gen.contains(&"m".to_string()) {
      let item = Item::Struct(generate_struct(table_info, &StructEnum::Stm));
      file.items.push(item);
    }
  }
  if *create {
    if !last_gen.contains(&"c".to_string()) {
      let item = Item::Struct(generate_struct(table_info, &StructEnum::Create));
      file.items.push(item);
      let item = Item::Fn(generate_insert_function(table_info));
      file.items.push(item);
    }
  }
  if *retrieve {
    if !last_gen.contains(&"Retrieve".to_string()) {
      let mut item = generate_retrieve_functions(table_info);
      match item {
        Ok(items) => {
          let mut items = items.into_iter().map(|tm| Item::Fn(tm)).collect::<Vec<_>>();
          file.items.append(&mut items);
        }
        Err(e) => {
          println!("{:#?}", e);
        }
      }
    }
  }
  if *update {
    if !last_gen.contains(&"Update".to_string()) {
      let item = Item::Struct(generate_struct(table_info, &StructEnum::Update));
      file.items.push(item);
      let item = generate_update_function(table_info, SafeOp::IgnoreNone);
      match item {
        Ok(tm) => {file.items.push(Item::Fn(tm));}
        Err(e) => {println!("{:#?}", e)}
      }
      let item = generate_update_function(table_info, SafeOp::AcceptNone);
      match item {
        Ok(tm) => {file.items.push(Item::Fn(tm));}
        Err(e) => {println!("{:#?}", e)}
      }
    }
  }
  if *delete {
    if !last_gen.contains(&"Delete".to_string()) {
      let item = generate_delete_function(table_info);
      file.items.push(Item::Fn(item));
    }
  }

  op
}




fn generate_expand_struct(table_info: &TableInfo) -> ItemStruct {
  let mut conflict_cols = get_conflict_cols(table_info);
  let conflict_cols = conflict_cols.drain(..).into_iter().filter(|col| !table_info.primary_keys.contains(col) && !table_info.foreign_key.iter().any(|t| t.target_keys.contains(&col))).collect::<Vec<_>>();

  let mut duplicate_cols = Vec::new();

  let struct_name_ident = Ident::new(&format!("{}Expand", table_info.table_name.to_pascal_case()), proc_macro2::Span::call_site());
  let mut fields = table_info
    .columns
    .iter()
    .map(|column| {
      let field_name = format_ident!("{}", column.column_name.to_snake_case());
      let field_type = parse_type(&column.data_type, column.is_nullable);
      Ok(quote! {
                #field_name: #field_type
            }
      )
    })
    .collect::<anyhow::Result<Vec<_>>>()
    .unwrap_or_default();

  for f in table_info.foreign_key.iter() {
    let mut fields_expand = f.target_columns.iter()
      .filter(|t| {
        if conflict_cols.iter().find(|col_name| **col_name == t.column_name).is_some() {
          duplicate_cols.push((&f.target_table, t.clone()));
          return false
        }
        !f.target_keys.contains(&t.column_name)
      })
      .map(|col| {
        let field_name = format_ident!("{}", col.column_name.to_snake_case());
        let field_type = parse_type(&col.data_type, col.is_nullable);
        quote! {
          #field_name: #field_type
        }
    }).collect::<Vec<_>>();
    fields.append(&mut fields_expand);
  }


  let mut duplicate_cols = duplicate_cols.iter().map(|(name,col)| {
    let field_name = format_ident!("{}_{}", name.to_snake_case(), col.column_name.to_snake_case());
    let field_type = parse_type(&col.data_type, col.is_nullable );
    quote! {
          #field_name: #field_type
        }
  }).collect::<Vec<_>>();

  fields.append(&mut duplicate_cols);

  parse_quote! {
    pub struct #struct_name_ident {
      #(#fields),*
    }
  }

}

pub async fn process_gen(options: &Options, tables: &Vec<TableInfo>) {
  let gen_tasks = tables
    .into_iter()
    .filter_map(|table| Some(gen_model_file(options, table)))
    .collect::<Vec<_>>();
  let res = tokio::try_join!(futures::future::try_join_all(gen_tasks));
  match res {
    Ok(_) => {}
    Err(e) => {
      println!("generate error: {:#?}", e);
    }
  }
}

pub async fn gen_model_file(options: &Options, table: &TableInfo) -> anyhow::Result<()> {
  // let table = &table;
  let Options { mod_dir,create, update, delete, retrieve, crud, model, .. } = options;
  let mut use_items: Vec<_> = generate_use_deps(options, &table);

  let mut comment = " include: ".to_string();
  if *crud {
    comment.push_str("m, c, r, u, d");
  } else {
    if *model {
      comment.push_str("m, ")
    }
    if *create {
      comment.push_str("c, ")
    }
    if *retrieve {
      comment.push_str("r, ")
    }
    if *update {
      comment.push_str("u, ")
    }
    if *delete {
      comment.push_str("d, ")
    }
  }

  let col_enum = if *create {
    generate_col_enum(&table).to_token_stream()} else {TokenStream::new()};
  let table_struct = if *model {
    generate_struct(&table, &StructEnum::Stm).to_token_stream()} else {TokenStream::new()};
  let create_struct = if *create {generate_struct(&table, &StructEnum::Create).to_token_stream()} else {TokenStream::new()};
  let update_struct = if *update {generate_struct(&table, &StructEnum::Update).to_token_stream()} else {TokenStream::new()};
  let insert_fn = if *create {generate_insert_function(&table).to_token_stream()} else {TokenStream::new()};
  let update_fn_safe = if *update {generate_update_function(&table, SafeOp::IgnoreNone)
    .ok()
    .map_or(TokenStream::new(), |t| t.to_token_stream())} else {TokenStream::new()};
  let update_fn = if *update {generate_update_function(&table, SafeOp::AcceptNone)
    .ok()
    .map_or(TokenStream::new(), |t| t.to_token_stream())} else {TokenStream::new()};
  // let where_imp = generate_where_imp(&table);
  // let update_fn = generate_update_function(&table, SafeOp::AcceptNone);
  let delete_fn = if *delete {generate_delete_function(&table).to_token_stream()} else {TokenStream::new()};
  let retrieve_fns = if *retrieve {generate_retrieve_functions(&table)?} else {Vec::new()};



  let file: File = parse_quote! {
          #![doc = #comment]

          use serde::{Deserialize, Serialize};
          use sqlx::{Executor, FromRow, PgPool, Postgres};
          #(#use_items) *

          #col_enum

          #table_struct

          #create_struct

          #update_struct

          #insert_fn

          #update_fn

          #update_fn_safe

          #delete_fn

          #(#retrieve_fns) *

        };
  let path = mod_dir.join(format!("{}.rs", table.table_name));
  tokio::fs::write(path, prettyplease::unparse(&file)).await?;
  Ok(())
}

async fn read_all_files_async(dir_path: &PathBuf) -> anyhow::Result<HashMap<String, ParsedFile>> {
  let mut result = HashMap::new();
  let mut entries = tokio::fs::read_dir(&dir_path)
    .await
    .with_context(|| format!("Failed to read directory: {:?}", &dir_path))?;

  while let Some(entry) = entries
    .next_entry()
    .await
    .with_context(|| "Failed to read next directory entry")?
  {
    let path = entry.path();

    if !path.is_file() || path.file_name().unwrap() == "mod.rs" {
      continue;
    }

    // 只处理 .rs 文件
    let file_name = path
      .file_name()
      .and_then(|n| n.to_str())
      .with_context(|| format!("Invalid filename: {:?}", path))?;

    let Some(key) = file_name.strip_suffix(".rs") else {
      continue;
    };

    let content = tokio::fs::read_to_string(&path)
      .await
      .with_context(|| format!("Failed to read file: {:?}", path))?;

    let ast =
      parse_file(&content).with_context(|| format!("Failed to parse file: {:?}\nPlease check the doc comment if existing? it like //! include: in each file.", path))?;

    let parsed = ParsedFile {
      // oid: 0,
      name: key.to_string(),
      path: path.clone(),
      content,
      ast,
    };

    result.insert(key.to_string(), parsed);
  }

  Ok(result)
}

pub async fn generate_mod_rs(
  _: &PathBuf,
  mod_item_attrs: &Vec<TableInfo>,
) -> Vec<ItemMod> {
  let content = mod_item_attrs
    .iter()
    .filter(|t| !t.table_name.starts_with("_"))
    .map(|t| {
      let oid_comment = format!(" {}, {}, {}", &t.table_name, &t.oid, t.last_modified.with_timezone(&Shanghai).to_string().replace(" ", "_"));
      let table_name = format_ident!("{}", &t.table_name);
      let item: ItemMod = parse_quote! { #[doc = #oid_comment] pub mod #table_name; };
      item
    })
    .collect::<Vec<_>>();
  content
  // tokio::fs::write(mod_path, content).await;
}

pub async fn update_mod_rs(
  options: &Options,
  pg_class: HashMap<u32, SqlxMapping>,
  mut mod_file: File,
  item_attrs: &Vec<TableInfo>,
  map_vec: &Vec<SqlxMapping>,
) {
  for mod_item in mod_file.items.iter_mut() {
    match mod_item {
      Item::Mod(item) => {
        for attr in item.attrs.iter_mut() {
          if let Attribute {
            meta:
            Meta::NameValue(MetaNameValue {
                              value:
                              Expr::Lit(ExprLit {
                                          lit: Lit::Str(str), ..
                                        }),
                              ..
                            }),
            ..
          } = attr
          {
            let p = pg_class.iter().find(|(key, _)| {
              str.to_token_stream()
                .to_string()
                .contains(key.to_string().as_str())
            });
            match p {
              None => {}
              Some((oid, t)) => {
                let time = t.last_modified.with_timezone(&Shanghai);
                let map = map_vec.iter().find(|m| &m.oid.0 == oid);
                let comment = match map {
                  None => {
                    format!(" {}, {}, {}", t.table_name, oid, time.to_string().replace(" ", "_"))
                  }
                  Some(m) => {
                    let time_in_db = m.last_modified.with_timezone(&Shanghai);
                    if time_in_db > time {
                      format!(" {}, {}, {}", t.table_name, oid, time_in_db.to_string().replace(" ", "_"))
                  } else {
                      format!(" {}, {}, {}", t.table_name, oid, time.to_string().replace(" ", "_"))
                    }}
                };
                let lit_str: LitStr = parse_quote! { #comment };
                *str = lit_str;
              }
            }
          }
        }
      }
      _ => {}
    }
  }

  let Options {
    mod_file: mod_file_path,
    mod_dir, ..
  } = options;
  let mut mod_items = generate_mod_rs(mod_file_path, item_attrs)
    .await
    .into_iter()
    .map(|t| Item::Mod(t))
    .collect::<Vec<_>>();
  mod_file.items.append(&mut mod_items);

  create_nested_mod_files(mod_dir).await.expect("Failed to create nested mod");


  tokio::fs::write(mod_file_path, prettyplease::unparse(&mod_file))
    .await
    .expect("Unable to write file");
}

use tokio::io::AsyncWriteExt;

pub async fn create_nested_mod_files(path: &Path) -> anyhow::Result<()> {
  tokio::fs::create_dir_all(path).await?;

  let mut current = path.to_path_buf();
  while let Some(parent) = current.parent() {
    if parent == Path::new("") || parent == Path::new("src") || parent == Path::new("examples") {
      break;
    }
    let mod_file = parent.join("mod.rs");
    let module_name = current.file_name().unwrap().to_str().unwrap();
    let module_declaration = format!("pub mod {};\n", module_name);

    if !mod_file.exists() {
      tokio::fs::write(&mod_file, module_declaration).await?;
    } else {
      let content = tokio::fs::read_to_string(&mod_file).await?;
      if !content.contains(&module_declaration) {
        let mut file = OpenOptions::new()
          .append(true)
          .open(&mod_file)
          .await?;
        file.write_all(module_declaration.as_bytes()).await?;
      }
    }

    current = parent.to_path_buf();
  }

  Ok(())
}
#[derive(Clone, Debug)]
pub struct Options {
  pub mod_dir: PathBuf,
  pub mod_file: PathBuf,
  pub create: bool,
  pub update: bool,
  pub delete: bool,
  pub retrieve: bool,
  pub crud: bool,
  pub model: bool,
  pub db_url: String,
}


pub async fn update_file_name(
  mod_dir: &PathBuf,
  table: TableInfo,
  mut parsed_file: ParsedFile,
) -> anyhow::Result<(TableInfo, ParsedFile)> {

  let old_path = mod_dir.join(format!("{}.rs", &parsed_file.name));
  let new_path = mod_dir.join(format!("{}.rs", &table.table_name));
  parsed_file.name = table.table_name.clone();
  parsed_file.path = new_path.clone();
  tokio::fs::rename(old_path, new_path)
    .await
    .expect("Unable to rename file.");
  Ok((table, parsed_file))
}

pub async fn process_ast_update(options: &Options, mut tables: Vec<(TableInfo, ParsedFile)>) {
  let Options {  mod_dir, .. } = options;
  let tasks = tables
    .drain(..)
    .into_iter()
    .map(|(table,  parsed_file)| update_file_name(mod_dir, table, parsed_file))
    .collect::<Vec<_>>();

  let res = tokio::try_join!(futures::future::try_join_all(tasks));
  match res {
    Ok((in_file_tables, ..)) => {
      process_ast(options, in_file_tables).await;
    }
    Err(_) => {}
  }
}



#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
struct ForeignKey {
  constraint_name: String,
  source_table: String,
  source_keys: Vec<String>,
  target_table: String,
  target_keys: Vec<String>,
  #[sqlx(skip)]
  target_columns: Vec<ColumnInfo>,
}

async fn get_composite_foreign_keys(
  pool: &PgPool,
  schema_name: &str,
  table_name: &str
) -> Result<Vec<ForeignKey>, sqlx::Error> {
  let mut foreign_info = sqlx::query_as::<_, ForeignKey>(r#"
        SELECT
            con.conname AS constraint_name,
            src.relname AS source_table,
            ARRAY(
                SELECT attname
                FROM pg_attribute
                WHERE attrelid = con.conrelid
                AND attnum = ANY(con.conkey)
                ORDER BY array_position(con.conkey, attnum)
            ) AS source_keys,
            tgt.relname AS target_table,
            ARRAY(
                SELECT attname
                FROM pg_attribute
                WHERE attrelid = con.confrelid
                AND attnum = ANY(con.confkey)
                ORDER BY array_position(con.confkey, attnum)
            ) AS target_keys
        FROM
            pg_constraint con
            JOIN pg_class src ON con.conrelid = src.oid
            JOIN pg_class tgt ON con.confrelid = tgt.oid
        WHERE
            con.contype = 'f'
            AND src.relname = $1
    "#)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

  let foreign_info: Vec<_> = foreign_info.into_iter().map(async |mut f| {
    let target_table_name = &f.target_table;
    let cols = get_columns(pool, schema_name, &target_table_name).await.expect("");
    f.target_columns = cols;
    f
  }).collect();
  let (foreign_info, ..) = tokio::join!(futures::future::join_all(foreign_info));
  Ok(foreign_info)
}


#[cfg(test)]
mod tests {
  use crate::codegen::{ModItemAttr, Options, PgClass, SafeOp, StructEnum, generate_col_enum, generate_delete_function, generate_insert_function, generate_retrieve_functions, generate_struct, generate_update_function, get_tables, process_ast, process_ast_update, process_gen, scan, update_mod_rs, get_composite_foreign_keys, generate_use_deps, sqlx_mapping_init};

  use proc_macro2::TokenStream;
  use quote::ToTokens;
  use sqlx::postgres::PgPoolOptions;
  use std::path::PathBuf;
  use chrono::Utc;
  use syn::{File, parse_quote};


  #[tokio::test]
  async fn test_foreign() -> anyhow::Result<()> {
    //
    // let pool = PgPoolOptions::new()
    //   .max_connections(5)
    //   .connect(db_url)
    //   .await?;
    //
    // let tables = get_tables(&pool).await?;
    //
    // let res = generate_use_deps(tables.get("address").unwrap());
    //
    // let mut file: File = syn::parse_str("")?;
    // file.items.append(&mut res.into_iter().map(|t| syn::Item::Use(t)).collect::<Vec<_>>());
    //
    // println!("{:#?}", prettyplease::unparse(&file));

    Ok(())
  }
}


pub async fn sqlx_mapping_init(pool: &PgPool) -> Result<Vec<SqlxMapping>, sqlx::Error> {
  sqlx::query(
    r#"
       CREATE TABLE IF NOT EXISTS _sqlx_mapping (
        oid OID PRIMARY KEY,                      -- 使用表的OID作为主键
        database_name TEXT NOT NULL,              -- 数据库名
        schema_name TEXT NOT NULL,                -- 模式名(如public)
        table_name TEXT NOT NULL,                 -- 表名
        old_table_name TEXT NOT NULL,             -- 表名
        full_qualified_name TEXT GENERATED ALWAYS AS (database_name || '.' || schema_name || '.' || table_name) STORED,  -- 完整限定名
        last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT unique_table_identifier UNIQUE (database_name, schema_name, table_name)  -- 确保全库唯一
      );
        "#
  )
    .execute(pool)
    .await?;

  create_trigger(pool).await?;



  let tables = sqlx::query_as::<_, TableRow> (
    r#"
        SELECT
            table_name,
            table_catalog as db_name,
            table_schema as schema_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        "#,
  )
    .fetch_all(pool)
    .await?;

  let t_names: Vec<_> = tables
    .iter()
    .map(|table| table.table_name.clone())
    .collect();

  let pg_classes = sqlx::query_as!(
        PgClass,
        r#"SELECT oid, relname FROM pg_catalog.pg_class WHERE relname = ANY($1)"#,
        &t_names
    )
    .fetch_all(pool)
    .await
    .expect("");

    let mappings = get_matched_pairs(tables, pg_classes).into_iter().map(|(a, b)| {
      SqlxMapping {
        oid: b.oid,
        database_name: a.db_name.clone(),
        table_name: a.table_name.clone(),
        old_table_name: "".to_string(),
        schema_name: a.schema_name.clone(),
        full_qualified_name: format!("{}.{}.{}", &a.db_name, &a.schema_name, &a.table_name),
        last_modified: Utc::now(),
      }
    }).collect::<Vec<_>>();

  // 准备数组参数
  let (oids, db_names, schema_names, table_names, full_names, mod_times): (
    Vec<Oid>, Vec<&str>, Vec<&str>, Vec<&str>, Vec<&str>, Vec<chrono::DateTime<chrono::Utc>>,
  ) = mappings.iter().map(|m| (
    m.oid,
    m.database_name.as_str(),
    m.schema_name.as_str(),
    m.table_name.as_str(),
    m.full_qualified_name.as_str(),
    m.last_modified,
  )).multiunzip();

  sqlx::query(
    r#"
        INSERT INTO _sqlx_mapping (
            oid,
            database_name,
            schema_name,
            table_name,
            old_table_name,
--             full_qualified_name,
            last_modified
        )
        SELECT * FROM UNNEST(
            $1::oid[],
            $2::text[],
            $3::text[],
            $4::text[],
            $5::text[],
--             Default,
            $6::timestamptz[]
        )
        ON CONFLICT (oid)
        DO NOTHING
        RETURNING *
--         DO UPDATE SET
--             last_modified = EXCLUDED.last_modified
        "#
  )
    .bind(oids)
    .bind(db_names)
    .bind(schema_names)
    .bind(table_names.clone())
    .bind(table_names)
    .bind(mod_times)
    .execute(pool)
    .await?;

  let mappings = sqlx::query_as::<_, SqlxMapping>(r#"SELECT * FROM _sqlx_mapping"#).fetch_all(pool).await?;

  Ok(mappings)
}

async fn create_trigger(pool: &PgPool) -> Result<(), sqlx::Error> {

  sqlx::query(
    r#"
       CREATE OR REPLACE FUNCTION update_last_modified()
RETURNS event_trigger AS $$
BEGIN
    INSERT INTO _sqlx_mapping (
        oid,
        database_name,
        schema_name,
        table_name,
        old_table_name,
        last_modified
    )
    SELECT
        c.oid,
        current_database(),
        n.nspname,
        c.relname,
        c.relname,
        NOW()
    FROM pg_event_trigger_ddl_commands() e
    JOIN pg_class c ON e.objid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE e.object_type = 'table'
    ON CONFLICT (oid)
    DO UPDATE SET
        old_table_name = _sqlx_mapping.table_name,
        last_modified = EXCLUDED.last_modified,
        database_name = EXCLUDED.database_name,
        schema_name = EXCLUDED.schema_name,
        table_name = EXCLUDED.table_name;
END;
$$ LANGUAGE plpgsql;
        "#
  )
    .execute(pool)
    .await?;

  let trigger_exists: bool = sqlx::query_scalar(
    "SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtname = 'trg_table_modified')"
  )
    .fetch_one(pool)
    .await?;

  if !trigger_exists {
    sqlx::query(
      "CREATE EVENT TRIGGER trg_table_modified ON ddl_command_end
             WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE')
             EXECUTE FUNCTION update_last_modified()"
    )
      .execute(pool)
      .await?;
  }

  sqlx::query(
    r#"
        CREATE OR REPLACE FUNCTION clean_dropped_tables()
        RETURNS event_trigger AS $$
        BEGIN
            DELETE FROM _sqlx_mapping
            WHERE oid IN (
                SELECT objid
                FROM pg_event_trigger_dropped_objects()
                WHERE object_type = 'table'
            );
        END;
        $$ LANGUAGE plpgsql;
        "#
  )
    .execute(pool)
    .await?;

  let trigger_exists: bool = sqlx::query_scalar(
    "SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtname = 'trg_clean_dropped')"
  )
    .fetch_one(pool)
    .await?;

  if !trigger_exists {
    sqlx::query(
      "CREATE EVENT TRIGGER trg_clean_dropped ON sql_drop
             EXECUTE FUNCTION clean_dropped_tables()"
    )
      .execute(pool)
      .await?;
  }

  sqlx::query(r#"
-- 创建通知函数
CREATE OR REPLACE FUNCTION notify_sqlx_mapping_change()
RETURNS TRIGGER AS $$
DECLARE
    notification JSON;
BEGIN
    -- 构建通知内容
    notification = json_build_object(
        'event', TG_OP,
        'oid', COALESCE(NEW.oid, OLD.oid),
        'old', OLD,
        'new', NEW
    );

    -- 发送通知到特定频道
    PERFORM pg_notify('sqlx_mapping_changes', notification::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"#).execute(pool).await?;

  sqlx::query(
    r#"
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgname = 'trg_sqlx_mapping_notify'
                AND tgrelid = '_sqlx_mapping'::regclass
            ) THEN
                CREATE TRIGGER trg_sqlx_mapping_notify
                AFTER INSERT OR UPDATE OR DELETE ON _sqlx_mapping
                FOR EACH ROW EXECUTE FUNCTION notify_sqlx_mapping_change();
            END IF;
        END
        $$
        "#
  )
    .execute(pool)
    .await?;
  Ok(())
}

#[derive(FromRow)]
struct TableRow {
  table_name: String,
  db_name: String,
  schema_name: String,
}
fn get_matched_pairs(vec1: Vec<TableRow>, vec2: Vec<PgClass>) -> Vec<(TableRow, PgClass)> {
  vec1.into_iter()
    .sorted_by(|a, b| a.table_name.cmp(&b.table_name))
    .merge_join_by(
      vec2.into_iter().sorted_by(|a, b| a.relname.cmp(&b.relname)),
      |a, b| a.table_name.cmp(&b.relname),
    )
    .filter_map(|either| match either {
      itertools::EitherOrBoth::Both(a, b) => Some((a, b)),
      _ => None,
    })
    .collect()
}

#[derive(sqlx::FromRow, Clone, Debug)]
pub struct SqlxMapping {
  pub oid: Oid,
  pub database_name: String,
  pub table_name: String,
  pub old_table_name: String,
  pub schema_name: String,
  pub full_qualified_name: String,
  pub last_modified: chrono::DateTime<Utc>,
}


