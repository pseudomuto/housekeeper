CREATE FUNCTION `truncate_string` ON CLUSTER `production` AS (`str`, `max_len`) -> if(greater(length(`str`), `max_len`), concat(substring(`str`, 1, minus(`max_len`, 3)), '...'), `str`);
