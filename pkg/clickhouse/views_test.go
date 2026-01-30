package clickhouse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func TestNormalizeCastTwoArgToAs(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"simple", "CAST(x, 'String')", "CAST(x AS String)"},
		{"nullable decimal", "CAST(amt, 'Nullable(Decimal64(2))')", "CAST(amt AS Nullable(Decimal64(2)))"},
		{"nested expr", "CAST(foo(a, b), 'UInt32')", "CAST(foo(a, b) AS UInt32)"},
		{"already AS form", "CAST(x AS String)", "CAST(x AS String)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeCastTwoArgToAs(tt.in)
			if got != tt.want {
				t.Errorf("normalizeCastTwoArgToAs(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestCleanViewStatement_SimpleCase(t *testing.T) {
	// Test a simple case first
	originalQuery := `CREATE MATERIALIZED VIEW test.mv TO test.table (col1 String, col2 Int32) AS SELECT col1, col2 FROM source;`
	cleaned := cleanViewStatement(originalQuery)

	t.Logf("Original: %s", originalQuery)
	t.Logf("Cleaned: %s", cleaned)

	_, err := parser.ParseString(cleaned)
	if err != nil {
		t.Errorf("Failed to parse: %v", err)
	}
}

func TestCleanViewStatement_WithColumnDefinitions(t *testing.T) {
	// This is the exact case from the error message
	// ClickHouse returns CREATE statements with column definitions after TO clause
	originalQuery := `CREATE MATERIALIZED VIEW perceptor.raw_sessions_mv TO perceptor.raw_sessions_local (domain String, session_id String, browser_id String, min_event_received_at DateTime, max_event_received_at DateTime, referring_url AggregateFunction(argMin, String, DateTime), landing_url AggregateFunction(argMin, String, DateTime), exit_url AggregateFunction(argMax, String, DateTime), language AggregateFunction(argMin, String, DateTime), browser AggregateFunction(argMin, String, DateTime), browser_version AggregateFunction(argMin, String, DateTime), os AggregateFunction(argMin, String, DateTime), os_version AggregateFunction(argMin, String, DateTime), device_type AggregateFunction(argMin, String, DateTime), country_code AggregateFunction(argMin, String, DateTime), region_code AggregateFunction(argMin, String, DateTime), utm_source AggregateFunction(argMin, String, DateTime), utm_medium AggregateFunction(argMin, String, DateTime), utm_campaign AggregateFunction(argMin, String, DateTime), utm_term AggregateFunction(argMin, String, DateTime), utm_content AggregateFunction(argMin, String, DateTime), page_visit_ids AggregateFunction(groupUniqArray, String), page_view_count AggregateFunction(uniq, String), checkout_completed SimpleAggregateFunction(max, UInt8), conversion_funnel_depth SimpleAggregateFunction(max, Nullable(UInt8)), visited_urls AggregateFunction(groupUniqArray, String), visited_page_groups AggregateFunction(groupUniqArrayArray, Array(String)), clicked_selectors AggregateFunction(groupUniqArray, String), clicked_text AggregateFunction(groupUniqArray, String), viewed_product_skus AggregateFunction(groupUniqArray, String), viewed_product_titles AggregateFunction(groupUniqArray, String), viewed_product_types AggregateFunction(groupUniqArray, String), viewed_product_vendors AggregateFunction(groupUniqArray, String), product_view_count SimpleAggregateFunction(sum, UInt64), viewed_collection_titles AggregateFunction(groupUniqArray, String), collection_view_count SimpleAggregateFunction(sum, UInt64), search_queries AggregateFunction(groupUniqArray, String), search_count SimpleAggregateFunction(sum, UInt64), added_to_cart_skus AggregateFunction(groupUniqArray, String), added_to_cart_product_titles AggregateFunction(groupUniqArray, String), added_to_cart_product_types AggregateFunction(groupUniqArray, String), added_to_cart_product_vendors AggregateFunction(groupUniqArray, String), added_to_cart_count SimpleAggregateFunction(sum, UInt64), total_items_added_quantity SimpleAggregateFunction(sum, UInt64), total_items_added_value SimpleAggregateFunction(sum, UInt64), removed_from_cart_skus AggregateFunction(groupUniqArray, String), removed_from_cart_product_titles AggregateFunction(groupUniqArray, String), removed_from_cart_product_types AggregateFunction(groupUniqArray, String), removed_from_cart_product_vendors AggregateFunction(groupUniqArray, String), removed_from_cart_count SimpleAggregateFunction(sum, UInt64), max_cart_value SimpleAggregateFunction(max, Nullable(Decimal64(2))), max_cart_quantity SimpleAggregateFunction(max, Nullable(UInt32)), final_cart_viewed_quantity AggregateFunction(argMax, Nullable(UInt32), DateTime), final_cart_viewed_value AggregateFunction(argMax, Nullable(Decimal64(2)), DateTime), cart_currency SimpleAggregateFunction(any, Nullable(String)), cart_view_count SimpleAggregateFunction(sum, UInt64), checkout_started_count SimpleAggregateFunction(sum, UInt64), payment_info_submitted_count SimpleAggregateFunction(sum, UInt64), checkout_start_product_skus AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_start_product_titles AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_start_product_types AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_start_product_vendors AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_start_subtotal_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_start_shipping_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_start_tax_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_start_total_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_start_discount_codes_applied AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_start_discount_types_applied AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_start_discount_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), has_discount SimpleAggregateFunction(max, UInt8), order_id SimpleAggregateFunction(anyIf, Nullable(String)), customer_id SimpleAggregateFunction(anyIf, Nullable(String)), checkout_complete_product_skus AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_complete_product_titles AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_complete_product_types AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_complete_product_vendors AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_complete_product_quantities AggregateFunction(argMaxIf, Array(UInt32), DateTime, UInt8), checkout_complete_subtotal_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_complete_shipping_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_complete_tax_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_complete_total_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_complete_discount_value AggregateFunction(argMaxIf, Nullable(Decimal64(2)), DateTime, UInt8), checkout_complete_discount_codes_applied AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), checkout_complete_discount_types_applied AggregateFunction(argMaxIf, Array(String), DateTime, UInt8), visual_error_snippets AggregateFunction(groupUniqArrayArray, Array(String)), median_lcp AggregateFunction(quantile, Decimal64(2)), median_cls AggregateFunction(quantile, Decimal64(2)), median_inp AggregateFunction(quantile, Decimal64(2)), max_lcp SimpleAggregateFunction(max, Nullable(Decimal64(2))), max_cls SimpleAggregateFunction(max, Nullable(Decimal64(2))), max_inp SimpleAggregateFunction(max, Nullable(Decimal64(2))), ws_path SimpleAggregateFunction(any, String)) AS SELECT domain, session_id, any(browser_id) AS browser_id, min(event_received_at) AS min_event_received_at, max(event_received_at) AS max_event_received_at, argMinState(referring_url, event_received_at) AS referring_url, argMinState(event_url, event_received_at) AS landing_url, argMaxState(event_url, event_received_at) AS exit_url, argMinState(language, event_received_at) AS language, argMinState(browser, event_received_at) AS browser, argMinState(browser_version, event_received_at) AS browser_version, argMinState(os, event_received_at) AS os, argMinState(os_version, event_received_at) AS os_version, argMinState(device_type, event_received_at) AS device_type, argMinState(country_code, event_received_at) AS country_code, argMinState(region_code, event_received_at) AS region_code, argMinState(extractURLParameter(raw_events_local.referring_url, 'utm_source'), event_received_at) AS utm_source, argMinState(extractURLParameter(raw_events_local.referring_url, 'utm_medium'), event_received_at) AS utm_medium, argMinState(extractURLParameter(raw_events_local.referring_url, 'utm_campaign'), event_received_at) AS utm_campaign, argMinState(extractURLParameter(raw_events_local.referring_url, 'utm_term'), event_received_at) AS utm_term, argMinState(extractURLParameter(raw_events_local.referring_url, 'utm_content'), event_received_at) AS utm_content, groupUniqArrayState(page_visit_id) AS page_visit_ids, uniqState(page_visit_id) AS page_view_count, max(ecomm_name = 'checkout_completed') AS checkout_completed, max(multiIf(ecomm_name = 'product_added_to_cart', 1, ecomm_name = 'checkout_started', 2, ecomm_name = 'payment_info_submitted', 3, ecomm_name = 'checkout_completed', 4, NULL)) AS conversion_funnel_depth, groupUniqArrayState(cutQueryStringAndFragment(url)) AS visited_urls, groupUniqArrayArrayState(event_page_groups) AS visited_page_groups, groupUniqArrayState(if(user_step_selector != '', user_step_selector, NULL)) AS clicked_selectors, groupUniqArrayState(if(user_step_alt != '', user_step_alt, NULL)) AS clicked_text, groupUniqArrayState(if(ecomm_name = 'product_viewed', nullIf(JSONExtractString(ecomm_data, 'productVariant', 'sku'), ''), NULL)) AS viewed_product_skus, groupUniqArrayState(if(ecomm_name = 'product_viewed', nullIf(JSONExtractString(ecomm_data, 'productVariant', 'title'), ''), NULL)) AS viewed_product_titles, groupUniqArrayState(if(ecomm_name = 'product_viewed', nullIf(JSONExtractString(ecomm_data, 'productVariant', 'product', 'type'), ''), NULL)) AS viewed_product_types, groupUniqArrayState(if(ecomm_name = 'product_viewed', nullIf(JSONExtractString(ecomm_data, 'productVariant', 'product', 'vendor'), ''), NULL)) AS viewed_product_vendors, sum(if(ecomm_name = 'product_viewed', 1, 0)) AS product_view_count, groupUniqArrayState(if(ecomm_name = 'collection_viewed', nullIf(JSONExtractString(ecomm_data, 'collection', 'title'), ''), NULL)) AS viewed_collection_titles, sum(if(ecomm_name = 'collection_viewed', 1, 0)) AS collection_view_count, groupUniqArrayState(if(ecomm_name = 'search_submitted', nullIf(JSONExtractString(ecomm_data, 'searchResult', 'query'), ''), NULL)) AS search_queries, sum(if(ecomm_name = 'search_submitted', 1, 0)) AS search_count, groupUniqArrayState(if(ecomm_name = 'product_added_to_cart', nullIf(JSONExtractString(ecomm_data, 'cartLine', 'merchandise', 'sku'), ''), NULL)) AS added_to_cart_skus, groupUniqArrayState(if(ecomm_name = 'product_added_to_cart', nullIf(JSONExtractString(ecomm_data, 'cartLine', 'merchandise', 'product', 'title'), ''), NULL)) AS added_to_cart_product_titles, groupUniqArrayState(if(ecomm_name = 'product_added_to_cart', nullIf(JSONExtractString(ecomm_data, 'cartLine', 'merchandise', 'product', 'type'), ''), NULL)) AS added_to_cart_product_types, groupUniqArrayState(if(ecomm_name = 'product_added_to_cart', nullIf(JSONExtractString(ecomm_data, 'cartLine', 'merchandise', 'product', 'vendor'), ''), NULL)) AS added_to_cart_product_vendors, sum(if(ecomm_name = 'product_added_to_cart', 1, 0)) AS added_to_cart_count, sum(if(ecomm_name = 'product_added_to_cart', JSONExtractUInt(ecomm_data, 'cartLine', 'quantity'), 0)) AS total_items_added_quantity, sum(if(ecomm_name = 'product_added_to_cart', JSONExtractUInt(ecomm_data, 'cartLine', 'cost', 'totalAmount', 'amount'), 0)) AS total_items_added_value, groupUniqArrayState(if(ecomm_name = 'product_removed_from_cart', nullIf(JSONExtractString(ecomm_data, 'cartLine', 'merchandise', 'sku'), ''), NULL)) AS removed_from_cart_skus, groupUniqArrayState(if(ecomm_name = 'product_removed_from_cart', nullIf(JSONExtractString(ecomm_data, 'cartLine', 'merchandise', 'product', 'title'), ''), NULL)) AS removed_from_cart_product_titles, groupUniqArrayState(if(ecomm_name = 'product_removed_from_cart', nullIf(JSONExtractString(ecomm_data, 'cartLine', 'merchandise', 'product', 'type'), ''), NULL)) AS removed_from_cart_product_types, groupUniqArrayState(if(ecomm_name = 'product_removed_from_cart', nullIf(JSONExtractString(ecomm_data, 'cartLine', 'merchandise', 'product', 'vendor'), ''), NULL)) AS removed_from_cart_product_vendors, sum(if(ecomm_name = 'product_removed_from_cart', 1, 0)) AS removed_from_cart_count, max(if(ecomm_name = 'cart_viewed', JSONExtract(ecomm_data, 'cart', 'cost', 'totalAmount', 'amount', 'Nullable(Decimal64(2))'), NULL)) AS max_cart_value, max(if(ecomm_name = 'cart_viewed', JSONExtract(ecomm_data, 'cart', 'totalQuantity', 'Nullable(UInt32)'), NULL)) AS max_cart_quantity, argMaxState(if(ecomm_name = 'cart_viewed', JSONExtract(ecomm_data, 'cart', 'totalQuantity', 'Nullable(UInt32)'), NULL), event_received_at) AS final_cart_viewed_quantity, argMaxState(if(ecomm_name = 'cart_viewed', JSONExtract(ecomm_data, 'cart', 'cost', 'totalAmount', 'amount', 'Nullable(Decimal64(2))'), NULL), event_received_at) AS final_cart_viewed_value, any(nullIf(multiIf(ecomm_name IN ('checkout_started'), JSONExtractString(ecomm_data, 'checkout', 'currencyCode'), ecomm_name = 'cart_viewed', JSONExtractString(ecomm_data, 'cart', 'cost', 'totalAmount', 'currencyCode'), ecomm_name = 'product_added_to_cart', JSONExtractString(ecomm_data, 'cartLine', 'cost', 'totalAmount', 'currencyCode'), ''), '')) AS cart_currency, sum(if(ecomm_name = 'cart_viewed', 1, 0)) AS cart_view_count, sum(if(ecomm_name = 'checkout_started', 1, 0)) AS checkout_started_count, sum(if(ecomm_name = 'payment_info_submitted', 1, 0)) AS payment_info_submitted_count, argMaxIfState(arrayMap(lineItem -> JSONExtractString(lineItem, 'variant', 'sku'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_product_skus, argMaxIfState(arrayMap(lineItem -> JSONExtractString(lineItem, 'title'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_product_titles, argMaxIfState(arrayMap(lineItem -> JSONExtractString(lineItem, 'variant', 'product', 'type'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_product_types, argMaxIfState(arrayMap(lineItem -> JSONExtractString(lineItem, 'variant', 'product', 'vendor'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_product_vendors, argMaxIfState(JSONExtract(ecomm_data, 'checkout', 'subtotalPrice', 'amount', 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_subtotal_value, argMaxIfState(JSONExtract(ecomm_data, 'checkout', 'shippingLine', 'price', 'amount', 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_shipping_value, argMaxIfState(JSONExtract(ecomm_data, 'checkout', 'totalTax', 'amount', 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_tax_value, argMaxIfState(JSONExtract(ecomm_data, 'checkout', 'totalPrice', 'amount', 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_total_value, argMaxIfState(arrayMap(discount -> JSONExtractString(discount, 'title'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'discountApplications')), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_discount_codes_applied, argMaxIfState(arrayMap(discount -> JSONExtractString(discount, 'type'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'discountApplications')), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_discount_types_applied, argMaxIfState(CAST(arraySum(arrayMap(lineItem -> arraySum(arrayMap(allocation -> ifNull(toDecimal64OrNull(JSONExtractString(JSONExtractRaw(allocation, 'amount'), 'amount'), 2), 0), JSONExtractArrayRaw(lineItem, 'discountAllocations'))), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems'))), 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_started') AS checkout_start_discount_value, max(if(ecomm_name IN ('checkout_started', 'payment_info_submitted', 'checkout_completed'), length(JSONExtractArrayRaw(ecomm_data, 'checkout', 'discountApplications')) > 0, false)) AS has_discount, any(if(ecomm_name = 'checkout_completed', JSONExtractString(ecomm_data, 'checkout', 'order', 'id'), NULL)) AS order_id, any(if(ecomm_name = 'checkout_completed', JSONExtractString(ecomm_data, 'checkout', 'order', 'customer', 'id'), NULL)) AS customer_id, argMaxIfState(arrayMap(lineItem -> JSONExtractString(lineItem, 'variant', 'sku'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_product_skus, argMaxIfState(arrayMap(lineItem -> JSONExtractString(lineItem, 'title'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_product_titles, argMaxIfState(arrayMap(lineItem -> JSONExtractString(lineItem, 'variant', 'product', 'type'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_product_types, argMaxIfState(arrayMap(lineItem -> JSONExtractString(lineItem, 'variant', 'product', 'vendor'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_product_vendors, argMaxIfState(arrayMap(lineItem -> toUInt32OrZero(JSONExtractString(lineItem, 'quantity')), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems')), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_product_quantities, argMaxIfState(JSONExtract(ecomm_data, 'checkout', 'subtotalPrice', 'amount', 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_subtotal_value, argMaxIfState(JSONExtract(ecomm_data, 'checkout', 'shippingLine', 'price', 'amount', 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_shipping_value, argMaxIfState(JSONExtract(ecomm_data, 'checkout', 'totalTax', 'amount', 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_tax_value, argMaxIfState(JSONExtract(ecomm_data, 'checkout', 'totalPrice', 'amount', 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_total_value, argMaxIfState(CAST(arraySum(arrayMap(lineItem -> arraySum(arrayMap(allocation -> ifNull(toDecimal64OrNull(JSONExtractString(JSONExtractRaw(allocation, 'amount'), 'amount'), 2), 0), JSONExtractArrayRaw(lineItem, 'discountAllocations'))), JSONExtractArrayRaw(ecomm_data, 'checkout', 'lineItems'))), 'Nullable(Decimal64(2))'), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_discount_value, argMaxIfState(arrayMap(discount -> JSONExtractString(discount, 'title'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'discountApplications')), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_discount_codes_applied, argMaxIfState(arrayMap(discount -> JSONExtractString(discount, 'type'), JSONExtractArrayRaw(ecomm_data, 'checkout', 'discountApplications')), event_received_at, ecomm_name = 'checkout_completed') AS checkout_complete_discount_types_applied, groupUniqArrayArrayState(if(event_type = 'visual_error', visual_matches.snippet, [])) AS visual_error_snippets, quantileState(if((event_type = 'wv') AND (vital_name = 'LCP'), toDecimal64OrNull(vital_value, 2), NULL)) AS median_lcp, quantileState(if((event_type = 'wv') AND (vital_name = 'CLS'), toDecimal64OrNull(vital_value, 2), NULL)) AS median_cls, quantileState(if((event_type = 'wv') AND (vital_name = 'INP'), toDecimal64OrNull(vital_value, 2), NULL)) AS median_inp, max(if((event_type = 'wv') AND (vital_name = 'LCP'), toDecimal64OrNull(vital_value, 2), NULL)) AS max_lcp, max(if((event_type = 'wv') AND (vital_name = 'CLS'), toDecimal64OrNull(vital_value, 2), NULL)) AS max_cls, max(if((event_type = 'wv') AND (vital_name = 'INP'), toDecimal64OrNull(vital_value, 2), NULL)) AS max_inp, any(ws_path) AS ws_path FROM perceptor.raw_events_local GROUP BY domain, session_id;`

	cleaned := cleanViewStatement(originalQuery)

	// Debug: Check if column definitions were removed
	t.Logf("Original query length: %d", len(originalQuery))
	t.Logf("Cleaned query length: %d", len(cleaned))
	t.Logf("First 200 chars of cleaned: %s", cleaned[:min(200, len(cleaned))])

	// Check that column definitions were removed (cleaned has "AS SELECT" and no column defs)
	if strings.Contains(cleaned, "domain String, session_id String") {
		t.Errorf("Column definitions were not removed from the query")
	}

	// First, try to parse just the SELECT part to see if that's the issue
	selectStart := strings.Index(cleaned, "AS SELECT")
	if selectStart > 0 {
		selectPart := cleaned[selectStart+3:] // Skip "AS "
		_, err := parser.ParseString(selectPart)
		if err != nil {
			t.Logf("SELECT part alone fails to parse: %v", err)
		} else {
			t.Logf("SELECT part alone parses successfully")
		}
	}

	// The cleaned query should be parseable
	_, err := parser.ParseString(cleaned)
	if err != nil {
		// Show context around the error position (8925)
		errorPos := 8925
		start := max(0, errorPos-100)
		end := min(len(cleaned), errorPos+100)
		t.Errorf("Failed to parse cleaned query: %v\nContext around error position %d:\n...%s...\n\nFull cleaned query (first 1000 chars):\n%s",
			err,
			errorPos,
			cleaned[start:end],
			cleaned[:min(1000, len(cleaned))])
		return
	}

	// The cleaned query should still contain the TO clause and AS SELECT
	if !strings.Contains(cleaned, "TO perceptor.raw_sessions_local") {
		t.Errorf("TO clause was incorrectly removed")
	}
	if !strings.Contains(cleaned, "AS SELECT") {
		t.Errorf("AS SELECT was incorrectly removed")
	}
}

// TestViewExtractionAndFormatting_ReplicatesMigrationIssue tests the full extraction
// and formatting flow to ensure extracted views match existing migrations exactly.
// This replicates the issue where housekeeper generates new migrations unnecessarily.
func TestViewExtractionAndFormatting_ReplicatesMigrationIssue(t *testing.T) {
	// Try to read the actual migration file for comparison
	var expectedFromMigration string
	migrationPaths := []string{
		"../../../db/migrations/20260127230348.sql",
		"../../../../data/explorations/db/migrations/20260127230348.sql",
		"/Users/dseel/noibu/unicron/data/explorations/db/migrations/20260127230348.sql",
	}

	for _, path := range migrationPaths {
		absPath, _ := filepath.Abs(path)
		if content, err := os.ReadFile(absPath); err == nil {
			expectedFromMigration = strings.TrimSpace(string(content))
			t.Logf("Read migration file from: %s", absPath)
			break
		}
	}

	if expectedFromMigration == "" {
		t.Skip("Could not read migration file - skipping test")
		return
	}

	// Parse and format the expected migration to see its "canonical" format
	expectedParsed, err := parser.ParseString(expectedFromMigration)
	if err != nil {
		t.Fatalf("Failed to parse expected migration: %v", err)
	}

	formatter := format.New(format.Defaults)
	var expectedFormattedBuf strings.Builder
	if err := formatter.Format(&expectedFormattedBuf, expectedParsed.Statements...); err != nil {
		t.Fatalf("Failed to format expected migration: %v", err)
	}
	expectedFormatted := expectedFormattedBuf.String()

	// Simulate what ClickHouse actually returns: the CREATE statement WITH column definitions
	// ClickHouse returns: CREATE MATERIALIZED VIEW ... TO table (col1 Type1, col2 Type2, ...) AS SELECT ...
	// We need to construct this by taking the migration and adding column definitions
	// The column definitions should match the SELECT clause columns

	// For a proper test, we'd need the actual ClickHouse response. For now, let's test
	// by constructing a simplified version with a few columns to see if the cleaning works
	// In reality, ClickHouse would return all columns with their AggregateFunction types

	// Construct a ClickHouse-style response: take the migration and inject column definitions
	// Format: CREATE ... TO table (col1 Type1, col2 Type2, ...) AS SELECT ...
	// We'll insert column definitions right before "AS SELECT"

	// Find "AS SELECT" in the migration
	asSelectIdx := strings.Index(expectedFromMigration, "AS SELECT")
	if asSelectIdx == -1 {
		t.Fatalf("Could not find 'AS SELECT' in migration")
	}

	// Construct column definitions based on the SELECT columns
	// For a real test, we'd need the actual ClickHouse response, but for now let's use
	// a simplified approach: just test that the cleaning process works
	// ClickHouse would return something like:
	// (domain String, session_id String, browser_id String, ...) AS SELECT ...

	// For debugging, let's use the migration as-is first to verify the pipeline works
	// Then we can add column definitions to test the real issue
	clickhouseResponse := expectedFromMigration

	// Step 1: Clean the view statement (remove column definitions)
	// Since the migration doesn't have column definitions, this should be a no-op
	cleaned := cleanViewStatement(clickhouseResponse)
	t.Logf("Step 1 - After cleanViewStatement:")
	t.Logf("  Length: %d -> %d", len(clickhouseResponse), len(cleaned))
	t.Logf("  First 200 chars: %s", cleaned[:min(200, len(cleaned))])

	// Step 2: Normalize using cleanCreateStatement (parse and reformat)
	normalized := cleanCreateStatement(cleaned)
	t.Logf("\nStep 2 - After cleanCreateStatement:")
	t.Logf("  Length: %d -> %d", len(cleaned), len(normalized))
	t.Logf("  First 200 chars: %s", normalized[:min(200, len(normalized))])

	// Step 3: Parse the normalized statement
	parsed, err := parser.ParseString(normalized)
	if err != nil {
		t.Fatalf("Failed to parse normalized statement: %v\nNormalized:\n%s", err, normalized)
	}

	if len(parsed.Statements) == 0 {
		t.Fatal("No statements parsed")
	}

	if parsed.Statements[0].CreateView == nil {
		t.Fatal("Parsed statement is not a CreateView")
	}

	// Step 4: Format the parsed statement
	var formattedBuf strings.Builder
	if err := formatter.Format(&formattedBuf, parsed.Statements...); err != nil {
		t.Fatalf("Failed to format parsed statement: %v", err)
	}

	formatted := formattedBuf.String()
	t.Logf("\nStep 3 - After formatting:")
	t.Logf("  Length: %d", len(formatted))
	t.Logf("  First 200 chars: %s", formatted[:min(200, len(formatted))])

	// Step 5: Compare with expected migration output
	// expectedFormatted was already computed above
	t.Logf("\nStep 4 - Expected (from migration, formatted):\n  Length: %d", len(expectedFormatted))
	t.Logf("  First 200 chars: %s", expectedFormatted[:min(200, len(expectedFormatted))])

	// Compare the formatted outputs
	if formatted != expectedFormatted {
		t.Errorf("Formatted output does not match expected migration output")
		t.Errorf("\n=== FORMATTED (extracted) ===\n%s\n", formatted)
		t.Errorf("\n=== EXPECTED (from migration) ===\n%s\n", expectedFormatted)

		// Show character-by-character differences
		showDiff(t, formatted, expectedFormatted, "formatted", "expected")
	} else {
		t.Logf("✓ Formatted output matches expected migration output exactly")
	}

	// Also verify the parsed structures are equivalent
	extractedView := parsed.Statements[0].CreateView
	expectedView := expectedParsed.Statements[0].CreateView

	if extractedView == nil || expectedView == nil {
		t.Fatal("One of the views is nil")
	}

	// Compare key properties
	if extractedView.Name != expectedView.Name {
		t.Errorf("View name mismatch: %s != %s", extractedView.Name, expectedView.Name)
	}

	if (extractedView.Database == nil) != (expectedView.Database == nil) {
		t.Errorf("Database presence mismatch")
	} else if extractedView.Database != nil && expectedView.Database != nil {
		if *extractedView.Database != *expectedView.Database {
			t.Errorf("Database mismatch: %s != %s", *extractedView.Database, *expectedView.Database)
		}
	}

	if extractedView.Materialized != expectedView.Materialized {
		t.Errorf("Materialized flag mismatch: %v != %v", extractedView.Materialized, expectedView.Materialized)
	}

	if (extractedView.To == nil) != (expectedView.To == nil) {
		t.Errorf("TO clause presence mismatch")
	} else if extractedView.To != nil && expectedView.To != nil {
		if (extractedView.To.Database == nil) != (expectedView.To.Database == nil) {
			t.Errorf("TO database presence mismatch")
		} else if extractedView.To.Database != nil && expectedView.To.Database != nil {
			if *extractedView.To.Database != *expectedView.To.Database {
				t.Errorf("TO database mismatch: %s != %s", *extractedView.To.Database, *expectedView.To.Database)
			}
		}
		if (extractedView.To.Table == nil) != (expectedView.To.Table == nil) {
			t.Errorf("TO table presence mismatch")
		} else if extractedView.To.Table != nil && expectedView.To.Table != nil {
			if *extractedView.To.Table != *expectedView.To.Table {
				t.Errorf("TO table mismatch: %s != %s", *extractedView.To.Table, *expectedView.To.Table)
			}
		}
	}

	t.Logf("✓ All view properties match")
}

func showDiff(t *testing.T, actual, expected, actualLabel, expectedLabel string) {
	actualLines := strings.Split(actual, "\n")
	expectedLines := strings.Split(expected, "\n")

	maxLines := max(len(actualLines), len(expectedLines))
	differences := 0
	maxDiffShow := 10

	for i := 0; i < maxLines && differences < maxDiffShow; i++ {
		var actualLine, expectedLine string
		if i < len(actualLines) {
			actualLine = actualLines[i]
		}
		if i < len(expectedLines) {
			expectedLine = expectedLines[i]
		}

		if actualLine != expectedLine {
			t.Logf("Line %d differs:", i+1)
			t.Logf("  %s: %q", actualLabel, actualLine)
			t.Logf("  %s: %q", expectedLabel, expectedLine)
			differences++
		}
	}

	if differences >= maxDiffShow {
		t.Logf("... (showing first %d differences)", maxDiffShow)
	}
}
