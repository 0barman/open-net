use super::*;

pub(super) struct AbortOnDrop(pub(super) JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
