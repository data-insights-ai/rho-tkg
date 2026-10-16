// Command gen writes the v4 export and expected-answer files of every
// scenario: go run ./cmd/gen -out testdata
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
)

func main() {
	out := flag.String("out", "testdata", "output directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o750); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, s := range fixtures.Scenarios() {
		o, err := fixtures.Generate(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", s.Name, err)
			os.Exit(1)
		}
		for name, data := range map[string][]byte{s.Name + ".export": o.Export, s.Name + ".expected.json": o.Expected} {
			if err := os.WriteFile(filepath.Join(*out, name), data, 0o600); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Printf("%s %d bytes\n", name, len(data))
		}
	}
}
