/*
* MultiLine Comments are Supported!
*/
-- housekeeper:import schemas/common.sql
-- This is a comprehensive test of comment support across all DDL types
-- Create the main database

CREATE DATABASE `analytics` ENGINE = Atomic COMMENT 'Analytics database';

-- Create a users table
-- This table stores user information

CREATE TABLE `analytics`.`users` (
    `id`    UInt64,
    `name`  String,
    `email` String
)
ENGINE = MergeTree()
ORDER BY `id`;

-- Create a dictionary for user lookups
-- Uses HTTP source for data

CREATE DICTIONARY `analytics`.`user_dict` (
    `id`   UInt64,
    `name` String
)
PRIMARY KEY `id`
SOURCE(HTTP(url 'http://api.example.com/users'))
LAYOUT(HASHED())
LIFETIME(3600)
COMMENT 'User lookup dictionary';

-- Create a materialized view for analytics
-- Aggregates daily user activity

CREATE MATERIALIZED VIEW `analytics`.`daily_stats`
ENGINE = MergeTree() ORDER BY `date`
AS SELECT
    toDate(`timestamp`) AS `date`,
    count() AS `users`
FROM `analytics`.`events`
GROUP BY `date`;

-- Create a helper function
-- Multiplies a number by two

CREATE FUNCTION `double` AS (`x`) -> multiply(`x`, 2);

-- Attach a backup database

ATTACH DATABASE `backup` ENGINE = Memory;

-- Detach temporary database

DETACH DATABASE `temp_db` PERMANENTLY;

-- Drop old database

DROP DATABASE `old_analytics` SYNC;

-- Rename databases

RENAME DATABASE `staging` TO `production`;

-- Create admin role for database management

CREATE ROLE `db_admin`;

-- housekeeper:import some/file.sql
-- Grant privileges to admin role
-- This allows full database management

GRANT ALL ON `analytics`.* TO `db_admin` WITH GRANT OPTION;
