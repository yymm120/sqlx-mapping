use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Deserialize, Default, Clone)]
pub struct Config {
    #[serde(default)]
    pub type_mappings: HashMap<String, TypeMapping>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TypeMapping {
    #[serde(rename = "type")]
    pub rust_type: String,
    pub feature: Option<String>,
}

pub async fn load_config() -> Result<Config> {
    let content = fs::read_to_string("Cargo.toml")
        .context("Failed to load config! please check Cargo.toml!")?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}

pub fn map_type(sql_type: &str, is_nullable: bool) -> Result<String> {
    // let config = load_config()?;

    // if let Some(mapping) = config.type_mappings.get(&sql_type.to_lowercase()) {
    //     return Ok(if is_nullable {
    //         format!("Option<{}>", mapping.rust_type)
    //     } else {
    //         mapping.rust_type.clone()
    //     });
    // }

    // Default mappings
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
        _ => return Err(anyhow!(format!("Unsupported SQL type: {}", sql_type))),
    };

    if is_nullable {
        Ok(format!("Option<{}>", rust_type))
    } else {
        Ok(rust_type.to_string())
    }
}
