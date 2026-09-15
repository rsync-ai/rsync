-- Admin control over which alerts are emailed, and an admin-managed alert list.
--
-- Migration 102 gave the admin per-category mutes for Slack only. Email was
-- decided entirely by each pipeline owner on /settings, so the admin page could
-- not stop, say, schema-drift email instance-wide, and there was no way to send
-- alerts to an address that is not a user account (an on-call inbox, a team
-- distribution list).
--
-- email_muted_categories: categories the admin blocked for email. A blocked
-- category is emailed to NOBODY, whatever the owner chose. For every other
-- category the owner's own switch and mutes (user_notification_preferences)
-- still apply to the owner's copy. Stored as a MUTED list for the same reason
-- as slack_muted_categories: a category added in a later release is delivered
-- by default rather than silently dropped.
--
-- email_extra_recipients: bare addresses (validated and de-duplicated in Go,
-- at most 20) that receive every email alert the admin has not blocked,
-- regardless of any user's mutes. A recipient who is also the pipeline owner
-- gets one copy, not two.
--
-- Both default to empty, so an instance that saved channels under 102 keeps
-- emailing exactly as before.

ALTER TABLE notification_channel_settings
    ADD COLUMN IF NOT EXISTS email_muted_categories TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS email_extra_recipients TEXT[] NOT NULL DEFAULT '{}';

COMMENT ON COLUMN notification_channel_settings.email_muted_categories IS
'Categories the admin blocked for email: never emailed to anyone, whatever the owner chose.';

COMMENT ON COLUMN notification_channel_settings.email_extra_recipients IS
'Admin-managed addresses that receive every email alert not blocked by email_muted_categories, regardless of user mutes. Max 20, validated in Go.';
