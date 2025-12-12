SELECT
    `category`,
    count(*)
FROM `products`
GROUP BY `category` WITH ROLLUP;
