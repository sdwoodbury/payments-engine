use std::{error::Error, fmt, fmt::Formatter};

#[macro_export]
macro_rules! fmt_error {
    ($($arg:tt)*) => {{
        let res = format!($($arg)*);
        format!("[{}:{}] {}", file!(), line!(), res)
    }}
}

pub fn print_report<T>(report: error_stack::Report<T>) {
    // want to view the source of the error first
    let stack: Vec<&error_stack::Frame> = report.frames().collect();
    for frame in stack.iter().rev() {
        //let location = frame.location();
        //let file_line = format!("[{}:{}] ", location.file(), location.line());
        let msg = match frame.kind() {
            error_stack::FrameKind::Context(context) => format!("Error: {}", context),
            error_stack::FrameKind::Attachment(error_stack::AttachmentKind::Printable(
                attachment,
            )) => format!("- {}", attachment),
            _ => "".to_string(),
        };
        log::error!("{}", msg);
    }
}

#[derive(Debug)]
pub enum MyError {
    Conversion(String),
    Db,
    FileReader,
    Generic(&'static str),
    GenericFmt(String),
}

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for MyError {}
