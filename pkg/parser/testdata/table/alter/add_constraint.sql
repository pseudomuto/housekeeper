ALTER TABLE `users`
    ADD CONSTRAINT `id_check` CHECK `id` > 0;
