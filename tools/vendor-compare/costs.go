package vendorcompare

// Metric explicitly distinguishes measured zero from unavailable/incomplete data.
type Metric struct {
	Status string  `json:"status"`
	Value  *uint64 `json:"value"`
	Unit   string  `json:"unit"`
	Scope  string  `json:"scope"`
	Reason string  `json:"reason,omitempty"`
}

// Inventory counts native rows. Declared topology is not replica verification.
type Inventory struct {
	LogicalNodes         int    `json:"logical_nodes"`
	LogicalEdges         int    `json:"logical_edges"`
	ForwardRows          int    `json:"native_forward_edge_rows"`
	ReverseRows          int    `json:"native_reverse_edge_rows"`
	DeclaredCopies       int    `json:"declared_data_copies"`
	DeclaredPartitions   int    `json:"declared_partitions"`
	NodeAttributesPerRow int    `json:"declared_schema_attributes_per_node"`
	EdgeAttributesPerRow int    `json:"declared_schema_attributes_per_edge"`
	Scope                string `json:"scope"`
}

// DiskFile is one whole-volume regular file, including fixed installation costs.
type DiskFile struct {
	Name           string `json:"relative_name"`
	ApparentBytes  uint64 `json:"apparent_bytes"`
	AllocatedBytes uint64 `json:"allocated_bytes"`
	DeviceInode    string `json:"device_inode"`
}

// Costs keeps nonoverlapping cgroup totals separate from overlapping diagnostics.
type Costs struct {
	Complete               bool              `json:"complete"`
	CgroupMemoryCurrent    Metric            `json:"cgroup_memory_current"`
	CgroupMemoryPeak       Metric            `json:"cgroup_memory_peak"`
	CgroupMemoryStat       map[string]uint64 `json:"cgroup_memory_stat"`
	CgroupCPUUsec          Metric            `json:"cgroup_cpu_usage"`
	ProcessTreeRSS         Metric            `json:"process_tree_rss"`
	ProcessTreePSS         Metric            `json:"process_tree_pss"`
	HeapAllocator          Metric            `json:"heap_allocator"`
	HomeVolumeApparent     Metric            `json:"whole_home_volume_apparent"`
	HomeVolumeAllocated    Metric            `json:"whole_home_volume_allocated"`
	HomeVolumeFiles        []DiskFile        `json:"whole_home_volume_files"`
	ContainerWritableLayer Metric            `json:"container_writable_layer"`
	DockerStdoutLogs       Metric            `json:"docker_stdout_logs"`
	ImageProvisioning      Metric            `json:"image_provisioning"`
	ClientMemory           Metric            `json:"client_memory"`
	ClientDisk             Metric            `json:"client_disk"`
	AllReplicaTotal        Metric            `json:"all_replica_total"`
	Unmeasured             []string          `json:"unmeasured"`
	Accounting             string            `json:"accounting"`
	ObservationErrors      []string          `json:"observation_errors"`
}

func unknown(unit, scope, reason string) Metric {
	return Metric{Status: "unavailable", Unit: unit, Scope: scope, Reason: reason}
}
func measured(value uint64, unit, scope string) Metric {
	return Metric{Status: "measured", Value: new(value), Unit: unit, Scope: scope}
}
func unavailableCosts(reason string) Costs {
	return Costs{CgroupMemoryCurrent: unknown("bytes", "all services in owned container", reason), CgroupMemoryPeak: unknown("bytes", "owned container lifecycle high-water", reason),
		CgroupCPUUsec: unknown("microseconds", "all owned container services", reason), ProcessTreeRSS: unknown("bytes", "owned container process tree", "RSS diagnostic not collected"),
		ProcessTreePSS: unknown("bytes", "owned container process tree", "PSS not collected"), HeapAllocator: unknown("bytes", "individual native/JVM services", "allocator instrumentation absent"),
		HomeVolumeApparent: unknown("bytes", "complete /home/tigergraph volume", reason), HomeVolumeAllocated: unknown("bytes", "complete /home/tigergraph volume", reason),
		ContainerWritableLayer: unknown("bytes", "owned container writable layer", reason), DockerStdoutLogs: unknown("bytes", "Docker VM stdout/stderr log file", "host VM log allocation not observed"),
		ImageProvisioning: unknown("bytes", "shared image layers and downloads", "image provisioning separate and not measured"), ClientMemory: unknown("bytes", "Go adapter/driver client process", "RSS/PSS and peaks unmeasured"),
		ClientDisk: unknown("bytes", "client exports/normalizer/reference workspace", "workspace peak/allocation ledger absent"), AllReplicaTotal: unknown("bytes", "nonoverlapping complete deployment total", "only one declared local copy; complete disk/client/replica categories unavailable"),
		Unmeasured: []string{"process-tree RSS/PSS", "native heap/allocator categories", "disk bytes by graph/index/WAL/snapshot/catalog/log category", "peak/temporary disk", "Docker stdout logs", "image provisioning", "adapter/client RSS/PSS/CPU/disk peaks", "host/VM overhead", "replica/coordinator verification", "durability/fault acknowledgements", "latency/throughput"}, Accounting: "Never sum heap/RSS/mapped pages/file cache with cgroup memory.current. Whole home volume includes installation, data, indexes, WAL, snapshots, configs and logs; semantic categorization is unavailable. Declared one copy is not a measured replicated deployment."}
}
