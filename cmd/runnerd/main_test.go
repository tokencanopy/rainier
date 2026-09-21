// cmd/runnerd/main_test.go
package main

import (
	"strings"
	"testing"
)

// TestEgressProxyEndpoint covers the one flag that decides what a microVM
// guest's firewall lets out.
func TestEgressProxyEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		explicit string
		proxyURL string
		wantAddr string
		wantPort int
		wantErr  string
	}{
		{
			name:     "no proxy at all is a runner with no exception",
			wantAddr: "",
			wantPort: 0,
		},
		{
			name:     "an explicit address wins",
			explicit: "10.44.0.9:3128",
			proxyURL: "http://egressd:3129",
			wantAddr: "10.44.0.9",
			wantPort: 3128,
		},
		{
			name:     "a proxy URL that is already an address",
			proxyURL: "http://10.44.0.9:3129",
			wantAddr: "10.44.0.9",
			wantPort: 3129,
		},
		{
			name:     "a proxy URL with no port takes the scheme's",
			proxyURL: "https://10.44.0.9",
			wantAddr: "10.44.0.9",
			wantPort: 443,
		},
		{
			name:     "a proxy URL naming a host must be given as an address",
			proxyURL: "http://egressd:3129",
			wantErr:  "names a host and not an address",
		},
		{
			name:     "an explicit endpoint that is not ip:port",
			explicit: "10.44.0.9",
			wantErr:  "not ip:port",
		},
		{
			name:     "an explicit endpoint with a nonsense port",
			explicit: "10.44.0.9:not-a-port",
			wantErr:  "not a port",
		},
		{
			name:     "an IPv6 proxy no guest could reach",
			explicit: "[fd00::1]:3128",
			wantErr:  "IPv6",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr, port, err := egressProxyEndpoint(tc.explicit, tc.proxyURL)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("egressProxyEndpoint(%q, %q) = %q/%d, want an error naming %q", tc.explicit, tc.proxyURL, addr, port, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want it to name %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("egressProxyEndpoint(%q, %q): %v", tc.explicit, tc.proxyURL, err)
			}
			if addr != tc.wantAddr || port != tc.wantPort {
				t.Fatalf("egressProxyEndpoint(%q, %q) = %q/%d, want %q/%d", tc.explicit, tc.proxyURL, addr, port, tc.wantAddr, tc.wantPort)
			}
		})
	}
}
