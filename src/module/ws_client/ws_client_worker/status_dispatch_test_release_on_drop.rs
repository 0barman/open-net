pub(super) struct ReleaseOnDrop(pub(super) Option<std::sync::mpsc::Sender<()>>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        // Disconnecting releases the callback even when the test returns early.
        self.0.take();
    }
}
