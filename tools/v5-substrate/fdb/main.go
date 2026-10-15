// This executable is optional test support, not a rho storage backend/server.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func openDB() (fdb.Database, error) {
	if err := fdb.APIVersion(730); err != nil {
		return fdb.Database{}, err
	}
	connection := os.Getenv("FDB_CONNECTION_STRING")
	if connection == "" {
		return fdb.Database{}, fmt.Errorf("FDB_CONNECTION_STRING required; use run-local.sh")
	}
	db, err := fdb.OpenWithConnectionString(connection)
	if err != nil {
		return db, err
	}
	if err = db.Options().SetTransactionTimeout(10000); err == nil {
		err = db.Options().SetTransactionRetryLimit(20)
	}
	if err != nil {
		db.Close()
	}
	return db, err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT") != "" {
		if len(os.Args) != 2 || os.Args[1] != "workload-cell" && os.Args[1] != "feed-client-hold" && os.Args[1] != "feed-client-resume" {
			return errInvalid
		}
		if _, err := graphTransportFromEnv(); err != nil {
			return err
		}
	}
	if len(os.Args) == 2 && (os.Args[1] == "feed-client-hold" || os.Args[1] == "feed-client-resume") {
		db, err := openDB()
		if err != nil {
			return err
		}
		defer db.Close()
		return runGraphFeedChild(db, os.Args[1] == "feed-client-resume")
	}
	if len(os.Args) == 2 && os.Args[1] == "workload-cell" {
		raw := os.Getenv("RHO_GRAPH_WORKLOAD_SPEC")
		if len(raw) == 0 || len(raw) > 1024 {
			return errInvalid
		}
		var family struct {
			Family string `json:"family"`
		}
		if err := json.Unmarshal([]byte(raw), &family); err != nil {
			return errInvalid
		}
		if family.Family == graphFeedWorkloadFamily {
			spec, run, err := graphFeedWorkloadSpecFromEnv()
			if err != nil {
				return err
			}
			db, err := openDB()
			if err != nil {
				return err
			}
			defer db.Close()
			return runGraphFeedWorkload(db, spec, run)
		}
		if family.Family == graphEdgeUniqueFamily {
			spec, run, err := graphEdgeUniqueSpecFromEnv()
			if err != nil {
				return err
			}
			db, err := openDB()
			if err != nil {
				return err
			}
			defer db.Close()
			return runGraphEdgeUnique(db, spec, run)
		}
		if family.Family == graphSelectorsFamily {
			spec, run, err := graphSelectorsSpecFromEnv()
			if err != nil {
				return err
			}
			db, err := openDB()
			if err != nil {
				return err
			}
			defer db.Close()
			return runGraphSelectors(db, spec, run)
		}
		spec, run, err := graphContentionSpecFromEnv()
		if err != nil {
			return err
		}
		db, err := openDB()
		if err != nil {
			return err
		}
		defer db.Close()
		return runGraphContention(db, spec, run)
	}
	if len(os.Args) != 2 || (os.Args[1] != "smoke" && os.Args[1] != "graph-smoke" && os.Args[1] != "lifecycle-trace" && os.Args[1] != "cost-cell" && !strings.HasPrefix(os.Args[1], "fault-") && !strings.HasPrefix(os.Args[1], "graph-fault-") && !strings.HasPrefix(os.Args[1], "paired-fault-") && !strings.HasPrefix(os.Args[1], "platform-fault-") && !strings.HasPrefix(os.Args[1], "column-fault-") && !strings.HasPrefix(os.Args[1], "history-fault-")) {
		return fmt.Errorf("usage: fdb-spike smoke|graph-smoke|lifecycle-trace|cost-cell|fault-*|graph-fault-*|paired-fault-*|platform-fault-*|column-fault-*|history-fault-*")
	}
	db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()
	if os.Args[1] == "cost-cell" {
		spec, err := graphCostSpecFromEnv()
		if err != nil {
			return err
		}
		return runGraphCost(db, spec)
	}
	if strings.HasPrefix(os.Args[1], "history-fault-") {
		return runGraphHistoryFault(db, os.Args[1])
	}
	if strings.HasPrefix(os.Args[1], "column-fault-") {
		return runGraphColumnFault(db, os.Args[1])
	}
	if strings.HasPrefix(os.Args[1], "platform-fault-") {
		return runGraphPlatformFault(db, os.Args[1])
	}
	if strings.HasPrefix(os.Args[1], "paired-fault-") {
		return runPairedGraphFault(db, os.Args[1])
	}
	if os.Args[1] == "lifecycle-trace" {
		graph := os.Getenv("FDB_LIFECYCLE_GRAPH")
		if graph == "" {
			graph = "graph-lifecycle-seven"
		}
		h, err := newHarness(db, lifecycleConfig(graph))
		if err != nil {
			return err
		}
		return runLifecycleTrace(h)
	}
	if os.Args[1] == "graph-smoke" {
		graph := os.Getenv("FDB_GRAPH_FIXTURE")
		if graph == "" {
			graph = "graph-functional"
		}
		h, err := newHarness(db, graphConfig(graph))
		if err != nil {
			return err
		}
		return runGraphSmoke(h)
	}
	if strings.HasPrefix(os.Args[1], "graph-fault-") {
		return runGraphFault(db, os.Args[1])
	}
	if strings.HasPrefix(os.Args[1], "fault-") {
		return runFault(db, os.Args[1])
	}
	_, err = db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Set(fdb.Key("g0/smoke"), []byte("native-7.3.77"))
		tr.Set(fdb.Key("g1/smoke"), []byte("native-7.3.77"))
		return nil, nil
	})
	if err != nil {
		return err
	}
	result, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		version, err := tr.GetReadVersion().Get()
		if err != nil {
			return nil, err
		}
		values := map[string]string{}
		for _, key := range []string{"g0/smoke", "g1/smoke"} {
			b, err := tr.Get(fdb.Key(key)).Get()
			if err != nil {
				return nil, err
			}
			if string(b) != "native-7.3.77" {
				return nil, fmt.Errorf("smoke read mismatch %s", key)
			}
			values[key] = string(b)
		}
		return map[string]any{"api": 730, "read_version": version, "values": values}, nil
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
