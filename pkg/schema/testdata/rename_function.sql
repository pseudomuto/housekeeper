DROP FUNCTION IF EXISTS `calculate_percentage`;

CREATE FUNCTION `calc_percentage` AS (`part`, `total`) -> if(equals(`total`, 0), 0, divide(multiply(`part`, 100.0), `total`));

DROP FUNCTION IF EXISTS `old_multiply`;

CREATE FUNCTION `multiply_by_two` AS (`x`) -> multiply(`x`, 2);