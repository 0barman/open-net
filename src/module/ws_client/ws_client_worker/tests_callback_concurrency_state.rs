#[derive(Default)]
pub(super) struct CallbackConcurrencyState {
    pub(super) first_started: bool,
    pub(super) second_started: bool,
    pub(super) release_first: bool,
}
