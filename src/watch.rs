use sqlx::{Error, PgPool};
use sqlx::postgres::{PgListener, PgNotification};


#[derive(Debug)]
pub enum TableChangeEvent {
  Insert(serde_json::Value),
  Update(serde_json::Value),
  Delete(serde_json::Value),
}

#[derive(Debug, Clone)]
pub enum ChangeEvent {
  C, R, U, D, M, A
}

// async fn async_watch_table_changes(db_url: &str, options: &Options, rx: Receiver<ChangeEvent>, tx: Sender<String>) {
//   let url = db_url.to_string();
//   let handle = tokio::task::spawn(async move {
//     let pool = connect_db(url.as_str()).await;
//     match watch_table_changes(&pool, options, rx, tx).await {
//       Ok(_) => {}
//       Err(e) => {println!("{:#?}", e)}
//     };
//   });
//   handle.await.unwrap();
// }

pub async fn watch_table_changes(pool: &PgPool, options: Options, status: Arc<AtomicBool>, mut rx: Receiver<ChangeEvent>, tx: Sender<String>) {
  let d = Arc::new(AtomicU8::new(0));
  let tx_cn = tx.clone();
  if let Ok(mut listener) = PgListener::connect_with(pool).await {
    match listener.listen("sqlx_mapping_changes").await {
      Err(e) => {
        let _ = tx.send(e.to_string()).await;
      }
      _ => {}
    }
    tokio::spawn(async move {
      let mut stdout = io::stdout();
      let Options { mod_dir, mod_file, create, update, delete, retrieve, crud, model, db_url } = options;
      let executor = CommandExecutor;
      while let Some(event) = rx.recv().await {
        match event {
          ChangeEvent::C => {
            let out = executor.execute( "sqlx-mapping", &["map", "--output", &mod_dir.display().to_string(), "-c"]);
            match out {
              Ok(output) => {
                let _ = tx.send(output).await;
              }
              Err(e) => {
                let _ = tx.send(e.to_string()).await;
              }
            }
          }
          ChangeEvent::R => {
            let out = executor.execute( "sqlx-mapping", &["map", "--output", &mod_dir.display().to_string(), "-r"]);
            match out {
              Ok(output) => {
                let _ = tx.send(output).await;
              }
              Err(e) => {
                let _ = tx.send(e.to_string()).await;
              }
            }
          }
          ChangeEvent::U => {
            let out = executor.execute( "sqlx-mapping", &["map", "--output", &mod_dir.display().to_string(), "-u"]);
            match out {
              Ok(output) => {
                let _ = tx.send(output).await;
              }
              Err(e) => {
                let _ = tx.send(e.to_string()).await;
              }
            }
          }
          ChangeEvent::D => {
            let out = executor.execute( "sqlx-mapping", &["map", "--output", &mod_dir.display().to_string(), "-d"]);
            match out {
              Ok(output) => {
                let _ = tx.send(output).await;
              }
              Err(e) => {
                let _ = tx.send(e.to_string()).await;
              }
            }
          }
          ChangeEvent::M => {
            let out = executor.execute( "sqlx-mapping", &["map", "--output", &mod_dir.display().to_string(), "-m"]);
            match out {
              Ok(output) => {
                let _ = tx.send(output).await;
              }
              Err(e) => {
                let _ = tx.send(e.to_string()).await;
              }
            }
          }
          ChangeEvent::A => {
            let out = executor.execute( "sqlx-mapping", &["map", "--output", &mod_dir.display().to_string(), "-a"]);
            match out {
              Ok(output) => {
                let _ = tx.send(output).await;
              }
              Err(e) => {
                let _ = tx.send(e.to_string()).await;
              }
            }
          }
        }
      }
    });

    loop {
      let notification = listener.recv().await;
      match notification {
        Ok(n) => {
          println!("table changed: {}", n.payload());

          // 如果是update事件, 扫描mod文件, 如果oid 存在, 扫描文件名, 如果文件名不匹配, 更新文件名后执行更新逻辑
          // 如果文件名匹配, 执行更新逻辑
          // 如果是insert事件, 执行生成逻辑, 插入mod
          // 如果是delete事件, 不做任何操作
        }
        Err(e) => {let _ = tx_cn.send(e.to_string()).await;}
      }
    }
  }


}

use std::io::{self, Write};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicUsize, Ordering};
use crossterm::{execute, cursor, style, terminal, event::{self, Event, KeyCode, KeyEventKind}, queue, Command};
use tokio::sync::mpsc::{Receiver, Sender};

use which::which;
use crate::cli::connect_db;
use crate::codegen::Options;
use crate::executor::CommandExecutor;

#[derive(Clone)]
struct TOptions {
  event: ChangeEvent,
  name: &'static str,
  description:  &'static str,
  status: bool,
}

// 定义选项常量
const OPTIONS: [TOptions; 6] = [
  TOptions {
    event: ChangeEvent::M,
    name: "m",
    description: "Monitor DDL, update Rust struct in real-time",
    status: false,
  },
  TOptions {
    event: ChangeEvent::C,
    name: "c",
    description: "Monitor DDL, update insert function in real-time",
    status: false,
  },
  TOptions {
    event: ChangeEvent::R,
    name: "r",
    description: "Monitor DDL, update retrieve function in real-time",
    status: false,
  },
  TOptions {
    event: ChangeEvent::U,
    name: "u",
    description: "Monitor DDL, update update function in real-time",
    status: false,
  },
  TOptions {
    event: ChangeEvent::D,
    name: "d",
    description: "Monitor DDL, update delete function in real-time",
    status: false,
  },
  TOptions {
    event: ChangeEvent::A,
    name: "a",
    description: "Monitor DDL, update all structs and functions in real-time",
    status: false,
  },
];

const MENU: &str = r#"Sqlx Mapping Watching

Controls:

"#;

pub(crate) async fn run<W>(w: &mut W, options: Options) -> io::Result<char>
where
  W: io::Write + Send,
{

  let mut t_options = OPTIONS.into_iter().clone().collect::<Vec<_>>();
  let Options { mod_dir, mod_file, create, update, delete, retrieve, crud, model, db_url } = options.clone();



  let (task_tx, task_rx) = tokio::sync::mpsc::channel::<ChangeEvent>(10);
  let (print_tx, mut print_rx) = tokio::sync::mpsc::channel::<String>(20);
  let task_tx_clone = task_tx.clone();

  let status = Arc::new(AtomicBool::new(false));
  let status_spawn1 = status.clone();
  let status_spawn2 = status.clone();
  let status_main = status.clone();
  execute!(w, terminal::EnterAlternateScreen)?;
  terminal::enable_raw_mode()?;

  tokio::spawn(async move {
    let mut stdout = io::stdout();
    loop {
      if let Some(out) =  print_rx.recv().await {
        while !status_spawn1.load(Ordering::SeqCst) {}
          let _ = queue!(stdout, style::Print(&out)).expect("print failed.");
        stdout.flush().unwrap();
      }
    }
  });
  tokio::spawn(
    async move {
      let pool = connect_db(&db_url.clone()).await;
      watch_table_changes(&pool, options.clone(),status_spawn2, task_rx, print_tx).await;
  });

  for o in t_options.iter_mut() {
    let a: &str = o.name;
    match a {
      "m" => {o.status = crud || create || update || delete || retrieve || model; if model {let _ = task_tx_clone.send(ChangeEvent::M).await;}}
      "c" => {o.status = crud || create; if create {let _ = task_tx_clone.send(ChangeEvent::C).await;}}
      "r" => {o.status = crud || retrieve; if retrieve {let _ = task_tx_clone.send(ChangeEvent::R).await;}}
      "u" => {o.status = crud || update; if update {let _ = task_tx_clone.send(ChangeEvent::U).await;}}
      "d" => {o.status = crud || delete; if delete {let _ = task_tx_clone.send(ChangeEvent::D).await;}}
      "a" => {o.status = crud; if crud {let _ = task_tx_clone.send(ChangeEvent::A).await;}}
      _ => {}
    }
  };

  loop {
    status_main.fetch_and(false, Ordering::SeqCst);
    queue!(
            w,
            style::ResetColor,
            cursor::Hide,
            cursor::MoveTo(1, 1),
            style::Print(MENU),
        )?;

    w.flush()?;
    for option in t_options.iter() {
      let line = format!(
        " {}  {}",
        option.name,
        option.description
      );
      if option.status {
        execute!(w, style::SetForegroundColor(style::Color::Green), style::Print(line), style::ResetColor, cursor::MoveToNextLine(1))?;
      } else {
        execute!(w, style::Print(line), cursor::MoveToNextLine(1))?;
      }
    }

    status_main.fetch_or(true, Ordering::SeqCst);
    let executor = CommandExecutor;

    match read_char(w)? {
      'm' => {
        t_options[0].status = !t_options[0].status;
        if t_options[0].status {
          let _ = task_tx.clone().send(ChangeEvent::M).await;
        }
      },
      'c' => {
        t_options[1].status = !t_options[1].status;
        if t_options[1].status {
          let _ = task_tx.clone().send(ChangeEvent::C).await;
        }
      },
      'r' => {
        t_options[2].status = !t_options[2].status;
        if t_options[2].status {
          let _ = task_tx.clone().send(ChangeEvent::R).await;
        }
      },
      'u' => {
        t_options[3].status = !t_options[3].status;
        if t_options[3].status {
          let _ = task_tx.clone().send(ChangeEvent::U).await;
        }
      },
      'd' => {
        t_options[4].status = !t_options[4].status;
        if t_options[4].status {
          let _ = task_tx.clone().send(ChangeEvent::D).await;
        }
      },
      'a' => {
        let temp = !t_options[5].status;
        t_options.iter_mut().for_each(|o| o.status = temp);
        if t_options[5].status {
          let _ = task_tx.clone().send(ChangeEvent::A).await;
        }
      },
      'q' => {
        execute!(w, cursor::SetCursorStyle::DefaultUserShape)?;
        break;
      }
      _ => {}
    };
  }


  // 清理和恢复
  execute!(
        w,
        style::ResetColor,
        cursor::Show,
        terminal::LeaveAlternateScreen
    )?;

  terminal::disable_raw_mode()?;
  Ok('q')
}

fn command_exists(command: &str) -> bool {
  which(command).is_ok()
}


pub fn read_char<W>(w: &mut W) -> io::Result<char> where W: io::Write {

  loop {

    if let Ok(Event::Key(event::KeyEvent {
                           code: KeyCode::Char(c),
                           kind: KeyEventKind::Press,
                           modifiers,
                           state: _,
                         })) = event::read()
    {
      if modifiers == event::KeyModifiers::CONTROL && c == 'c' {
        return Ok('q');
      }
      return Ok(c);
    }
  }
}
