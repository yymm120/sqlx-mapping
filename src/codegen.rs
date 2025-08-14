use anyhow::{Context, anyhow};
use inflector::Inflector;
use proc_macro2::{Ident, TokenStream};
use quote::{ToTokens, format_ident, quote};
use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;
use sqlx::{FromRow, PgPool};
use std::collections::HashMap;
use std::path::PathBuf;
use syn::{
    Attribute, Expr, ExprLit, File, Item, ItemEnum, ItemFn, ItemMod, ItemStruct, Lit, LitStr, Meta,
    MetaNameValue, Type, parse_file, parse_quote,
};

#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
pub struct TableInfo {
    pub oid: u32,
    pub db_name: String,
    pub schema_name: String,
    pub table_name: String,
    #[sqlx(skip)]
    pub columns: Vec<ColumnInfo>,
    #[sqlx(skip)]
    pub primary_keys: Vec<String>,
    #[sqlx(skip)]
    pub unique_constraints: Vec<Vec<String>>,
    #[sqlx(skip)]
    pub indexes: Vec<IndexInfo>,
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
pub async fn get_tables(pool: &PgPool) -> anyhow::Result<HashMap<String, TableInfo>> {
    let mut tables_map = HashMap::<String, TableInfo>::new();
    #[derive(FromRow)]
    struct TableRow {
        table_name: String,
        db_name: String,
        schema_name: String,
    }

    let tables = sqlx::query_as::<_, TableRow>(
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

    // let mut result = Vec::with_capacity(tables.len());
    for table in tables {
        if let (Ok(c), Ok(p), Ok(u), Ok(i)) = tokio::join!(
            get_columns(pool, &table.schema_name, &table.table_name),
            get_primary_keys(pool, &table.schema_name, &table.table_name),
            get_unique_constraints(pool, &table.table_name),
            get_indexes(pool, &table.table_name)
        ) {
            if table.table_name.starts_with("_sqlx") {
                continue;
            }
            let oid = pg_classes
                .iter()
                .find(|t| t.relname == table.table_name)
                .unwrap()
                .oid
                .0;
            tables_map.insert(
                table.table_name.to_snake_case(),
                TableInfo {
                    oid,
                    db_name: table.db_name,
                    schema_name: table.schema_name.clone(),
                    table_name: table.table_name.clone(),
                    columns: c,
                    primary_keys: p,
                    unique_constraints: u,
                    indexes: i,
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
fn generate_col_enum(table: &TableInfo) -> ItemEnum {
    // 生成枚举变体
    let variants = table.columns.iter().map(|col| {
        let variant_name = Ident::new(
            &col.column_name.to_pascal_case(),
            proc_macro2::Span::call_site(),
        );

        // 根据数据类型确定字段类型
        let field_type = parse_type(&col.data_type, false);

        quote! { #variant_name(#field_type) }
    });

    // 构建完整枚举定义
    parse_quote! {
        #[derive(Debug)]
        enum Col {
            #(#variants),*
        }
    }
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

    let fields = table
        .columns
        .iter()
        .filter(|col| match struct_type {
            StructEnum::Stm => true,
            StructEnum::Update => !col.is_generated && !col.is_identity,
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

    let output = parse_quote! {
        #derives
        pub struct #struct_name {
            #(
                #fields,
            )*
        }
    };
    output
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
            sqlx::query_as!(
                #return_type, #query ,
                #(#set_values,) *
                #(#where_values),*
            )
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
        .map(|col| format_ident!("{}", col.column_name.to_pascal_case()))
        .collect::<Vec<_>>();

    let table_full_name = format!(
        "{}.{}.{}",
        table.db_name, table.schema_name, table.table_name
    );
    let table_full_name =
        syn::parse_str::<syn::LitStr>(&format!(r#""{}""#, table_full_name)).unwrap();

    // 构建完整的函数
    parse_quote! {
        pub async fn insert<'a, E>(executor: E, data: &#struct_name) -> Result<#return_type, sqlx::Error>
        where
            E: sqlx::Executor<'a, Database = sqlx::Postgres>,
        {
            let mut cols: Vec<Col> = Vec::new();
            let mut column_names = Vec::new();
            let mut placeholders = Vec::new();
            // let full_name = #table_full_name

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

    Ok(functions)
}

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
            sqlx::query_as!(#struct_name,
                #query_str,
                #(#args),*
            )
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
            sqlx::query_as!(#struct_name,
                #query_content,
                #(#args),*
            )
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
}

async fn create_mod_rs_if_not_exists(mod_path: &PathBuf) -> anyhow::Result<()> {
    if tokio::fs::try_exists(mod_path).await? {
        Ok(())
    } else {
        tokio::fs::write(mod_path, "")
            .await
            .context("Failed to write mod.rs")
    }
}

pub async fn sync_mod_from_db(
    mod_path: &PathBuf,
    pool: &PgPool,
) -> (File, HashMap<u32, ModItemAttr>) {
    create_mod_rs_if_not_exists(mod_path)
        .await
        .expect("Failed to create mod.rs");
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
                            let split_v = info[1..&info.len() - 1].split(":").collect::<Vec<_>>();
                            let oid = split_v[1].parse::<u32>();
                            match oid {
                                Ok(oid) => {
                                    return Some(ModItemAttr {
                                        name: split_v[0].to_string(),
                                        oid,
                                        table_name: "".to_string(), // 占位
                                    });
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
        .filter_map(|item| item)
        .collect::<Vec<_>>();

    let oid_list: Vec<_> = mod_items.iter().map(|t| Oid(t.oid)).collect();
    let pg_classes = sqlx::query_as!(
        PgClass,
        r#"SELECT oid, relname FROM pg_catalog.pg_class WHERE oid = ANY($1)"#,
        &oid_list
    )
    .fetch_all(pool)
    .await
    .expect("");

    let a = mod_items
        .into_iter()
        .filter_map(|t| {
            let temp = pg_classes.iter().find(|p| p.oid.0 == t.oid);
            match temp {
                None => None,
                Some(p) => Some(ModItemAttr {
                    name: t.name.clone(),
                    table_name: p.relname.clone(),
                    oid: t.oid,
                }),
            }
        })
        .map(|item| (item.oid, item))
        .collect::<HashMap<_, _>>();

    (mod_file, a)
}

pub async fn scan(
    options: &Options,
    pool: &PgPool,
) -> anyhow::Result<(
    (File, HashMap<u32, ModItemAttr>),
    HashMap<String, TableInfo>,
    HashMap<String, ParsedFile>,
)> {
    let Options { mod_dir, mod_file } = options;
    tokio::fs::create_dir_all(mod_dir).await?;
    if let (pg_classes, Ok(all_tables), Ok(files)) = tokio::join!(
        sync_mod_from_db(mod_file, &pool),
        get_tables(&pool),
        read_all_files_async(mod_dir)
    ) {
        return Ok((pg_classes, all_tables, files));
    }
    Err(anyhow!("Failed Scan files."))
}

pub async fn update_model_ast(
    options: &Options,
    table_info: TableInfo,
    mut file: ParsedFile,
) -> anyhow::Result<()> {
    let Options { mod_dir, .. } = options;
    let table_info = &table_info;
    for item in &mut file.ast.items {
        match item {
            Item::Enum(item_enum) => {
                let enum_name = item_enum.ident.to_token_stream().to_string();
                if enum_name == format!("{}Col", file.name.to_pascal_case()) {
                    // 字段长度/名称/类型变更
                    *item = Item::Enum(generate_col_enum(table_info));
                    continue;
                }
            }
            Item::Fn(item_fn) => {
                let method_name = item_fn.sig.ident.to_token_stream().to_string();
                if method_name.to_snake_case() == "insert" {
                    *item = Item::Fn(generate_insert_function(table_info));
                    continue;
                } else if method_name.to_snake_case() == "delete" {
                    *item = Item::Fn(generate_delete_function(table_info));
                    continue;
                } else if method_name.to_snake_case() == "update" {
                    match generate_update_function(table_info, SafeOp::AcceptNone) {
                        Ok(update_fn) => {
                            *item = Item::Fn(update_fn);
                        }
                        Err(_) => {}
                    }
                    match generate_update_function(table_info, SafeOp::IgnoreNone) {
                        Ok(update_fn) => {
                            *item = Item::Fn(update_fn);
                        }
                        Err(_) => {}
                    }
                    continue;
                } else if method_name.to_snake_case().starts_with("retrieve") {
                    // *item = Item::Struct(generate_struct(table_info, &StructEnum::Update));
                    continue;
                }
            }
            Item::Struct(item_struct) => {
                let struct_name = item_struct.ident.to_token_stream().to_string();
                if struct_name.to_snake_case() == file.name {
                    *item = Item::Struct(generate_struct(table_info, &StructEnum::Stm));
                    continue;
                } else if struct_name.ends_with("ForCreate") {
                    *item = Item::Struct(generate_struct(table_info, &StructEnum::Create));
                    continue;
                } else if struct_name.ends_with("ForUpdate") {
                    *item = Item::Struct(generate_struct(table_info, &StructEnum::Update));
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

pub async fn process_gen(options: &Options, tables: Vec<TableInfo>) {
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

pub async fn gen_model_file(options: &Options, table: TableInfo) -> anyhow::Result<()> {
    let table = &table;
    let Options { mod_dir, .. } = options;
    let col_enum = generate_col_enum(&table);
    let table_struct = generate_struct(&table, &StructEnum::Stm);
    let create_struct = generate_struct(&table, &StructEnum::Create);
    let update_struct = generate_struct(&table, &StructEnum::Update);
    let insert_fn = generate_insert_function(&table);
    let update_fn_safe = generate_update_function(&table, SafeOp::IgnoreNone)
        .ok()
        .map_or(TokenStream::new(), |t| t.to_token_stream());
    let update_fn = generate_update_function(&table, SafeOp::AcceptNone)
        .ok()
        .map_or(TokenStream::new(), |t| t.to_token_stream());
    // let update_fn = generate_update_function(&table, SafeOp::AcceptNone);
    let delete_fn = generate_delete_function(&table);
    let retrieve_fns = generate_retrieve_functions(&table)?;

    let file: File = parse_quote! {
    //! =====================
    //!
    //! Meta Information:
    //! - name: 'paotui.public.delivery_task'
    //! - sync: true
    //! - Enum: [Col]
    //! - Struct: [DeliveryTask], [DeliveryTaskForCreate], [DeliveryTaskForUpdate]
    //! - methods: [insert], [update], [delete], [retrieve]
    //! - oid: #40238#
    //!
    //! Note: Please do not delete comments, the code generation tool relies on comments.
    //!
    //! Examples:
    //! ```rust
    //! fn main () {
    //!   let transaction = pool.begin();
    //!   let data = DeliveryForCreate::default();
    //!   model::insert(&mut *transaction, &data);
    //!   transaction.commit();
    //! }
    //! ```
    //!
    //! =====================
          use serde::{Deserialize, Serialize};
          use sqlx::{Executor, FromRow, PgPool, Postgres};

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
            parse_file(&content).with_context(|| format!("Failed to parse file: {:?}", path))?;

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
    mod_item_attrs: &Vec<ModItemAttr>,
) -> Vec<ItemMod> {
    let content = mod_item_attrs
        .iter()
        .filter(|t| !t.table_name.starts_with("_"))
        .map(|t| {
            let oid_comment = format!(" {}: {}", &t.table_name, &t.oid);
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
    pg_class: HashMap<u32, ModItemAttr>,
    mut mod_file: File,
    item_attrs: &Vec<ModItemAttr>,
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
                                let comment = format!(" {}: {}", t.table_name, oid);
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
        ..
    } = options;
    let mut mod_items = generate_mod_rs(mod_file_path, item_attrs)
        .await
        .into_iter()
        .map(|t| Item::Mod(t))
        .collect::<Vec<_>>();
    mod_file.items.append(&mut mod_items);

    tokio::fs::write(mod_file_path, prettyplease::unparse(&mod_file))
        .await
        .expect("Unable to write file");
}

pub struct Options {
    pub mod_dir: PathBuf,
    pub mod_file: PathBuf,
}
#[cfg(test)]
mod tests {
    use crate::codegen::{
        ModItemAttr, Options, PgClass, SafeOp, StructEnum, generate_col_enum,
        generate_delete_function, generate_insert_function, generate_retrieve_functions,
        generate_struct, generate_update_function, get_tables, process_ast, process_ast_update,
        process_gen, scan, update_mod_rs,
    };

    use proc_macro2::TokenStream;
    use quote::ToTokens;
    use sqlx::postgres::PgPoolOptions;
    use std::path::PathBuf;
    use syn::{Expr, File, Stmt, parse_quote};

    #[tokio::test]
    async fn test_generate_insert_function() -> Result<(), Box<dyn std::error::Error>> {
        let p = sqlx::postgres::PgPoolOptions::new()
            .connect("postgres://postgres:postgres@localhost/paotui")
            .await?;
        let mut tables = get_tables(&p).await?;
        let table = tables.get("delivery_task").unwrap();

        let col_enum = generate_col_enum(&table);
        let table_struct = generate_struct(&table, &StructEnum::Stm);
        let create_struct = generate_struct(&table, &StructEnum::Create);
        let update_struct = generate_struct(&table, &StructEnum::Update);
        let insert_fn = generate_insert_function(&table);
        let update_fn_safe = generate_update_function(&table, SafeOp::IgnoreNone)
            .ok()
            .map_or(TokenStream::new(), |t| t.to_token_stream());
        let update_fn = generate_update_function(&table, SafeOp::AcceptNone)
            .ok()
            .map_or(TokenStream::new(), |t| t.to_token_stream());
        // let update_fn = generate_update_function(&table, SafeOp::AcceptNone);
        let delete_fn = generate_delete_function(&table);
        let retrieve_fns = generate_retrieve_functions(&table)?;

        let file: File = parse_quote! {
          use serde::{Deserialize, Serialize};
          use sqlx::{Executor, FromRow, PgPool, Postgres};

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

        let path = PathBuf::new().join("examples/address_insert.rs");
        tokio::fs::write(path, prettyplease::unparse(&file)).await?;
        println!("{}", prettyplease::unparse(&file));
        Ok(())
    }

    #[tokio::test]
    async fn test_oid() -> Result<(), Box<dyn std::error::Error>> {
        let db_url = "postgres://postgres:postgres@localhost/paotui";
        let path_dir = PathBuf::new().join("examples/models");

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(db_url)
            .await?;

        let options = Options {
            mod_dir: path_dir.clone(),
            mod_file: path_dir.clone().join("mod.rs"),
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
