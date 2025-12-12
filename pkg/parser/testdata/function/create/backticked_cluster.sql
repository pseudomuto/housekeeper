CREATE FUNCTION `calc_percentage` ON CLUSTER `prod-cluster` AS (`part`, `total`) -> if(equals(`total`, 0), 0, divide(multiply(`part`, 100.0), `total`));
