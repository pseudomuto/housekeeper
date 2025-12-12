ALTER TABLE `measurements`
    MODIFY ORDER BY (`device_identifier`, `created_at`, `id`);
