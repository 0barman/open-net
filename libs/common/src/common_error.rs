#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommonError {
    None = -1,
    RuntimeError = 0,
    PostError = 1,
}
