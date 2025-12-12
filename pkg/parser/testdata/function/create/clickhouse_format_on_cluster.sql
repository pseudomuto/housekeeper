CREATE FUNCTION `normalizedOS` ON CLUSTER `warehouse` AS (`os`) -> multiIf(
    startsWith(lower(`os`), 'windows'), 'Windows',
    startsWith(lower(`os`), 'mac'), 'Mac',
    'Other'
);
