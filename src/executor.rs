use std::process::{Command, Stdio};
use std::io::{self, Write, BufRead, BufReader, Read, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use crossterm::{execute, cursor, style, terminal};
use which::which;

pub struct CommandExecutor;

impl CommandExecutor {
  pub fn execute(&self, cmd: &str, args: &[&str]) -> io::Result<String> {

    if !which(cmd).is_ok() {
      let message = format!("command '{}' not found!\n", cmd);
      return Err(io::Error::new(ErrorKind::InvalidInput, message));
    }

    match Command::new(cmd)
      .args(args)
      .stdout(Stdio::piped())
      .stderr(Stdio::piped())
      .spawn()
    {
      Ok(mut child) => {
        // 捕获标准输出
        if let Some(mut stdout) = child.stdout.take() {
          let mut str = "".into();
          stdout.read_to_string(&mut str)?;
          return Ok(str);
        }

        // 捕获错误输出
        if let Some(mut stderr) = child.stderr.take() {
          let mut str = "".into();
          stderr.read_to_string(&mut str)?;
          return Ok(str);
        }
      }
      Err(e) => {
        return Err(io::Error::new(ErrorKind::Other, e));
      }
    }

    Err(io::Error::new(ErrorKind::Other, "failed to execute command"))
  }
}