#[derive(Clone, Copy)]
pub(super) enum Reentry {
    Observe,
    Unregister,
    Register,
}
