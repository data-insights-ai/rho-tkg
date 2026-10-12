package badger

import (
	"errors"
	"slices"
	"testing"
)

// MetaSetDurable and ForEachMetaName (backlog 49: tiered's earlier-lives
// record): a durable meta write reads back on disk after a restart; the
// prefix walk yields exactly the names under the prefix, in key order, stops
// on a callback error, and fails closed on a nil callback or a closed store.
func TestMetaSetDurableAndForEachMetaName(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"p/b", "p/a", "q/a", "p", "pp/a"} {
		if err := bs.MetaSetDurable(k, []byte{1}); err != nil {
			t.Fatalf("MetaSetDurable(%q): %v", k, err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	names := func(prefix string) []string {
		t.Helper()
		var out []string
		if err := bs.ForEachMetaName(prefix, func(name string) error {
			out = append(out, name)
			return nil
		}); err != nil {
			t.Fatalf("ForEachMetaName(%q): %v", prefix, err)
		}
		return out
	}
	if got, want := names("p/"), []string{"p/a", "p/b"}; !slices.Equal(got, want) {
		t.Fatalf("ForEachMetaName(p/) = %v, want %v", got, want)
	}
	if got := names("none/"); got != nil {
		t.Fatalf("ForEachMetaName(none/) = %v, want none", got)
	}

	stop := errors.New("stop")
	calls := 0
	if err := bs.ForEachMetaName("p", func(string) error { calls++; return stop }); !errors.Is(err, stop) || calls != 1 {
		t.Fatalf("callback error: err %v after %d calls, want stop after 1", err, calls)
	}
	if err := bs.ForEachMetaName("p", nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("nil callback: err %v, want ErrInvalidStoreMutation", err)
	}

	mem := newTestBadgerStoreInMemory(t)
	if err := mem.MetaSetDurable("m", []byte{2}); err != nil {
		t.Fatalf("in-memory MetaSetDurable: %v", err)
	}
	if v, err := mem.MetaGet("m"); err != nil || !slices.Equal(v, []byte{2}) {
		t.Fatalf("in-memory MetaGet = %v, %v", v, err)
	}
	_ = mem.Close()
	if err := mem.MetaSetDurable("m", nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed MetaSetDurable: err %v, want ErrStoreClosed", err)
	}
	if err := mem.ForEachMetaName("", func(string) error { return nil }); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed ForEachMetaName: err %v, want ErrStoreClosed", err)
	}
}
