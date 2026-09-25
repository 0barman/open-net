pub(super) struct Release(pub(super) Option<std::sync::mpsc::Sender<()>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
