ALTER TABLE `events`
    ADD COLUMN `created_at` DateTime DEFAULT now(),
    ADD COLUMN `metadata.version` Array(UInt16),
    ADD COLUMN `metadata.tags` Array(Array(String));