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
	if len(os.Args) != 2 || (os.Args[1] != "smoke" && os.Args[1] != "graph-smoke" && !strings.HasPrefix(os.Args[1], "fault-") && !strings.HasPrefix(os.Args[1], "graph-fault-")) {
		return fmt.Errorf("usage: fdb-spike smoke|graph-smoke|fault-*|graph-fault-*")
	}
	db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()
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
