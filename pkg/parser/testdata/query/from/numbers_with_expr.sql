SELECT toFloat32(`number` % 10) AS `n`
FROM numbers(10)
WHERE `number` % 3 = 1;
