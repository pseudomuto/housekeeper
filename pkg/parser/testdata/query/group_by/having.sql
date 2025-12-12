SELECT
    `category`,
    count(*)
FROM `products`
GROUP BY `category`
HAVING count(*) > 10;
