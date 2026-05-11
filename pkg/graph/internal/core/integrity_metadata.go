package core

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"

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
