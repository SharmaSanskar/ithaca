package api

import "testing"

func TestValidateEndpointURL(t *testing.T) {
	ok := []string{
		"https://example.com/webhooks",
		"http://receiver-sim:8090/sink/a",
		"https://example.com:8443/hook?x=1",
	}
	for _, u := range ok {
		if msg := validateEndpointURL(u); msg != "" {
			t.Errorf("%q rejected: %s", u, msg)
		}
	}
	bad := []string{
		"",
		"ftp://example.com",
		"example.com/no-scheme",
		"https://",
		"https://user:pass@example.com/",
		"https://example.com/#frag",
		"javascript:alert(1)",
	}
	for _, u := range bad {
		if msg := validateEndpointURL(u); msg == "" {
			t.Errorf("%q accepted", u)
		}
	}
}
