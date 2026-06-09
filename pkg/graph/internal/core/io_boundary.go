package core

import "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"

func isNilInterfaceValue(v any) bool {
	return grapherr.IsNil(v)
}
