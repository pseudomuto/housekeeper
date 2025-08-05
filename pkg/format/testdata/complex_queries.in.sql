-- Complex SELECT statements
SELECT user_id, toDate(timestamp) AS date, count() AS events, uniq(session_id) AS sessions FROM analytics.events WHERE date >= '2024-01-01' GROUP BY user_id, date HAVING events > 10 ORDER BY events DESC LIMIT 100;

SELECT e.user_id, u.name, e.event_type, count(*) AS event_count FROM analytics.events AS e LEFT JOIN analytics.users_dict AS u ON e.user_id = u.id WHERE e.timestamp >= today() - INTERVAL 7 DAY GROUP BY e.user_id, u.name, e.event_type ORDER BY event_count DESC;

WITH daily_stats AS (
SELECT toDate(timestamp) AS date, count() AS total_events, uniq(user_id) AS unique_users FROM analytics.events GROUP BY date
), weekly_stats AS (
SELECT toStartOfWeek(date) AS week, sum(total_events) AS week_events, avg(unique_users) AS avg_users FROM daily_stats GROUP BY week
) SELECT week, week_events, round(avg_users, 2) AS avg_daily_users FROM weekly_stats ORDER BY week DESC LIMIT 12;