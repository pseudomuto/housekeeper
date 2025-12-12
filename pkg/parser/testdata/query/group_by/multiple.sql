SELECT
    `category`,
    `brand`,
    count(*)
FROM `products`
GROUP BY `category`, `brand`;
