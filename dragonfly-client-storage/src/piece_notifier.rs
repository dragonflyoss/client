/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Notify;

/// PieceNotifier notifies the waiters when an in-flight piece download completes
/// (finished, failed or its metadata deleted), so the waiters do not need to poll
/// the piece metadata on an interval.
///
/// The notifier is claimed just before the piece metadata is created when the
/// piece download starts, and removed (waking all its waiters) when the download
/// reaches a terminal state. The piece metadata remains the source of truth:
/// waiters must re-check it after being notified.
#[derive(Default)]
pub(crate) struct PieceNotifier {
    /// The notifiers of the in-flight pieces, keyed by the piece id.
    notifiers: DashMap<String, Arc<Notify>>,
}

/// Claim is the result of claiming the download of a piece.
pub(crate) enum Claim {
    /// The caller inserted the notifier and is the sole downloader of the piece.
    Owner,

    /// Another download of the piece is already in flight. The caller should
    /// wait on its notifier and re-check the piece metadata.
    InFlight(Arc<Notify>),
}

/// PieceNotifier notifies the waiters when an in-flight piece download completes
/// (finished, failed or its metadata deleted), so the waiters do not need to poll
/// the piece metadata on an interval.
impl PieceNotifier {
    /// Atomically claims the download of the piece when its download starts.
    /// Exactly one of the concurrent claimers becomes the owner; the others get
    /// the owner's notifier to wait on, so the same piece is never downloaded
    /// twice concurrently.
    pub(crate) fn claim(&self, piece_id: &str) -> Claim {
        match self.notifiers.entry(piece_id.to_string()) {
            Entry::Occupied(entry) => Claim::InFlight(entry.get().clone()),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(Notify::new()));
                Claim::Owner
            }
        }
    }

    /// Returns the notifier of the in-flight piece, or `None` if the piece is not
    /// being downloaded by this process.
    pub(crate) fn get(&self, piece_id: &str) -> Option<Arc<Notify>> {
        self.notifiers
            .get(piece_id)
            .map(|notifier| notifier.value().clone())
    }

    /// Removes the notifier of the piece and wakes all its waiters when the piece
    /// download completes.
    pub(crate) fn remove_and_notify(&self, piece_id: &str) {
        if let Some((_, notifier)) = self.notifiers.remove(piece_id) {
            notifier.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_piece_notifier_wakes_enabled_waiters() {
        let piece_notifier = PieceNotifier::default();
        let piece_id = "d3add1f66b0d0b8083f14479d6e181ec9e2b34cf07d4a1a2ee2fcf51d3a3f14a-0";
        assert!(piece_notifier.get(piece_id).is_none());

        assert!(matches!(piece_notifier.claim(piece_id), Claim::Owner));
        let notifier = piece_notifier.get(piece_id).unwrap();

        match piece_notifier.claim(piece_id) {
            Claim::InFlight(in_flight) => assert!(Arc::ptr_eq(&notifier, &in_flight)),
            Claim::Owner => panic!("expected the second claim to lose"),
        }

        let notified = notifier.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        piece_notifier.remove_and_notify(piece_id);
        tokio::time::timeout(Duration::from_secs(1), notified)
            .await
            .unwrap();

        assert!(piece_notifier.get(piece_id).is_none());
        piece_notifier.remove_and_notify(piece_id);
        assert!(matches!(piece_notifier.claim(piece_id), Claim::Owner));
    }
}
