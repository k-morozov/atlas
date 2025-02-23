#[derive(Debug, PartialEq)]
pub enum Error {
    IO(String),
    InvalidData(String),
    LogicError(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            Error::IO(msg) => write!(f, "IO error: {msg}"),
            Error::LogicError(msg) => write!(f, "Logic error: {msg}"),
        }
    }
}

#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => { $crate::errors::Error::InvalidData(format!($($args)*)).into() };
}

#[macro_export]
macro_rules! logicerr {
    ($($args:tt)*) => { $crate::errors::Error::LogicError(format!($($args)*)).into() };
}

pub type Result<T> = std::result::Result<T, Error>;

impl<T> From<Error> for Result<T> {
    fn from(er: Error) -> Self {
        Err(er)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err.to_string())
    }
}
