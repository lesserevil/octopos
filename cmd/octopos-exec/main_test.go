package main

import "testing"

func TestNormalizeHostname(t *testing.T) {
	tests := map[string]string{
		"OctoPOS":                "octopos",
		" shedwards-octo1 ":      "shedwards-octo1",
		"bad host/name":          "bad-host-name",
		"..cluster..":            "cluster",
		"DOCTYPE HTML PUBLIC...": "doctype-html-public",
		"0123456789012345678901234567890123456789012345678901234567890123456789": "012345678901234567890123456789012345678901234567890123456789012",
	}

	for input, want := range tests {
		if got := normalizeHostname(input); got != want {
			t.Fatalf("normalizeHostname(%q) = %q, want %q", input, got, want)
		}
	}
}
