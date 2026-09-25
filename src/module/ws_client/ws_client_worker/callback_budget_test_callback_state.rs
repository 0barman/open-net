use super::*;

#[derive(Clone, Default)]
pub(super) struct CallbackState {
    pub(super) active: usize,
    pub(super) peak: usize,
    pub(super) observations: Vec<Observation>,
}
