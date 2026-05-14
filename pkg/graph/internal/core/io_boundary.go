package core

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"

func isNilInterfaceValue(v any) bool {
	return grapherr.IsNil(v)
}
