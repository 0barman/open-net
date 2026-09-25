#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Observation {
    Started(u64, u8),
    Finished(u64, u8),
}
