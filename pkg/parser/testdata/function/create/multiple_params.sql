CREATE FUNCTION `safe_divide` AS (`a`, `b`) -> if(equals(`b`, 0), 0, divide(`a`, `b`));
