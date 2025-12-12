CREATE FUNCTION `parity_str` AS (`n`) -> if(modulo(`n`, 2), 'odd', 'even');
