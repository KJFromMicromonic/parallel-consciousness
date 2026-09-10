// Package gateway builds invoices from incoming payment events.
package gateway

import "example.com/twoservice/billing"

// defaultCurrency is what this service stamps on every invoice it builds.
//
// It is seeded WRONG on purpose. Round one of a gate run has to fail for a
// realistic reason, and this is the most realistic one there is: an agent
// inherits existing code, has no reason to doubt it, and the gate teaches it
// otherwise. The correct value is written nowhere in this module — see
// integration/currency_test.go for why.
const defaultCurrency = "EUR"

// Build turns a payment event into an invoice.
func Build(customer string, amountMinor int64) billing.Invoice {
	return billing.Invoice{Customer: customer, AmountMinor: amountMinor, Currency: defaultCurrency}
}
