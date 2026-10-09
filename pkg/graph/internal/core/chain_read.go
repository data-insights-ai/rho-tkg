package core

// afterCurrentRead runs chainReadHook (tests only) between a per-entity chain
// assembly's current-row read and its history read, with the entity's raw ID
// (backlog 32: a concurrent move landing in that window).
func (c *Core) afterCurrentRead(id int64) {
	if h := c.chainReadHook; h != nil {
		h(id)
	}
}
