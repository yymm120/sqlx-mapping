use sqlx::PgPool;
use sqlx::postgres::PgListener;

pub const SQLX_MAPPING_CHANGES: &'static str = "sqlx_mapping_changes";

#[derive(Debug)]
pub enum TableChangeEvent {
  Insert(serde_json::Value),
  Update(serde_json::Value),
  Delete(serde_json::Value),
}


pub async fn watch_table_changes(pool: &PgPool) -> Result<(), sqlx::Error> {
  let mut listener = PgListener::connect_with(pool).await?;
  listener.listen(SQLX_MAPPING_CHANGES).await?;

  loop {
    let notification = listener.recv().await?;
    println!("表变更通知: {}", notification.payload());
    // 解析payload获取具体变更信息
  }
}
