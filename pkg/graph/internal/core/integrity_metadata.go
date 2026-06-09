package core

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

func nodeIntegrityWithHash(base *types.NodeIntegrity, hash, prevHash string) *types.NodeIntegrity {
	ig := &types.NodeIntegrity{}
	if base != nil {
		ig = base.DeepCopy()
	}
	ig.Hash = hash
	ig.PrevHash = prevHash
	return ig
}

func relIntegrityWithHash(base *types.RelIntegrity, hash, prevHash string) *types.RelIntegrity {
	ig := &types.RelIntegrity{}
	if base != nil {
		ig = base.DeepCopy()
	}
	ig.Hash = hash
	ig.PrevHash = prevHash
	return ig
}
