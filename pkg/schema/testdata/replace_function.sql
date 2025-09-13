CREATE FUNCTION `calculate_tax` AS (`amount`) -> multiply(`amount`, 0.10);

CREATE FUNCTION `format_currency` AS (`value`, `currency`) -> concat(`currency`, ' ', toString(`value`));