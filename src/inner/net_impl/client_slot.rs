use super::client_entry::ClientEntry;

pub(super) enum ClientSlot {
    Creating,
    Ready(ClientEntry),
    /// Name remains reserved while shutdown/join completes, including if destroy is cancelled.
    Closing,
}
