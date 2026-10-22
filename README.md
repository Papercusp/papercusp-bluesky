# Bluesky provider

Install through Cupboard and enter a bsky.social account handle/DID and a dedicated
app password on its Integrations card. The host creates and refreshes the session;
the app password, access JWT and refresh JWT never reach this package. Other PDS
origins are not supported by this package version.

Notifications include mentions and replies. Author feed includes the connected
account's own posts and replies, excluding reposts. Both REST histories use opaque
cursors and URI/CID watermarks, including mid-walk restart. A missing saved marker
reports retention loss; a page bound refuses instead of silently skipping history.
This is not a firehose replay and cannot discover every historical edit.

Publishing is public, text only. Reply targets require both root and parent strong
references. The host idempotency key addresses a deterministic SHA-256 record key;
the package never retries an unconfirmed write. Delete uses the exact record CID
as `swapRecord`, refuses another account's URI and reports `deleted:false` on
`InvalidSwap`. No live-account verification has been performed.

Verification uses recorded HTTP fakes and the SDK conformance kit. Live verification pending.

Primary lexicons reviewed 2026-10-16: bluesky-social/atproto's
com.atproto.server.{createSession,refreshSession} and com.atproto.repo procedures;
bluesky-social/social-app's app.bsky.notification.listNotifications,
app.bsky.feed.getAuthorFeed and app.bsky.feed.post.
