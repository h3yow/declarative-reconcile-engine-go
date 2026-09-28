package reconcile

import (
	"fmt"
	"sort"
)

func sortStrings(s []string) { sort.Strings(s) }

func joinIDs(ids []ResourceID) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += " -> "
		}
		out += id
	}
	if out == "" {
		return "<unknown>"
	}
	return out
}

func fmtSprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }
