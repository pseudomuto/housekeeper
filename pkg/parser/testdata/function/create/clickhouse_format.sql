CREATE FUNCTION `normalizedBrowser` AS (`br`) -> multiIf(
    lower(`br`) = 'firefox', 'Firefox',
    lower(`br`) = 'edge', 'Edge',
    lower(`br`) = 'safari', 'Safari',
    lower(`br`) = 'chrome', 'Chrome',
    lower(`br`) = 'webview', 'Webview',
    'Other'
);
