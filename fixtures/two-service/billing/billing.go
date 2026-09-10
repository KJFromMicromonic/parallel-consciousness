// Package billing renders invoice lines for a customer statement.
package billing

import "fmt"

// Invoice is one line on a statement.
//
// Currency exists here from the start, deliberately. An earlier arrangement of
// this fixture gave billing the job of ADDING this field and gateway the job
// of setting it — so gateway could not compile until billing's change merged,
// and the two agents deadlocked rather than coordinated. Each half must build
// alone; the coordination this fixture is meant to exercise is about the
// agreed VALUE, not about whether the code compiles.
type Invoice struct {
	Customer    string
	AmountMinor int64
	Currency    string
}

// Render formats one invoice line. The currency is last so a failing spanning
// test can assert on the suffix and quote something readable.
func Render(inv Invoice) string {
	return fmt.Sprintf("%s: %d.%02d %s", inv.Customer, inv.AmountMinor/100, inv.AmountMinor%100, inv.Currency)
}
