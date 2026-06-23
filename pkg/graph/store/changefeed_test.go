package store

import "testing"

func TestChangeTagString(t *testing.T) {
	cases := map[ChangeTag]string{
		ChangeNodePut:             "NodePut",
		ChangeRelPut:              "RelPut",
		ChangeNodeDelete:          "NodeDelete",
		ChangeRelDelete:           "RelDelete",
		ChangeNodeHistoryVersion:  "NodeHistoryVersion",
		ChangeRelHistoryVersion:   "RelHistoryVersion",
		ChangeNodeHistoryTruncate: "NodeHistoryTruncate",
		ChangeRelHistoryTruncate:  "RelHistoryTruncate",
		ChangeMeta:                "Meta",
		ChangeClear:               "Clear",
		ChangeTag(0):              "ChangeTag(unknown)",
		ChangeTag(200):            "ChangeTag(unknown)",
	}
	for tag, want := range cases {
		if got := tag.String(); got != want {
			t.Errorf("ChangeTag(%d).String() = %q, want %q", byte(tag), got, want)
		}
	}
}

func TestChangeTagValid(t *testing.T) {
	for tag := ChangeNodePut; tag <= ChangeClear; tag++ {
		if !tag.Valid() {
			t.Errorf("tag %v should be valid", tag)
		}
	}
	for _, tag := range []ChangeTag{0, ChangeClear + 1, 255} {
		if tag.Valid() {
			t.Errorf("tag %d should be invalid", byte(tag))
		}
	}
}

// Tag numbers are durable on disk: this pins them so a careless renumber fails
// the build's test gate rather than silently misreading every persisted record.
func TestChangeTagWireNumbersStable(t *testing.T) {
	pinned := map[ChangeTag]byte{
		ChangeNodePut:             1,
		ChangeRelPut:              2,
		ChangeNodeDelete:          3,
		ChangeRelDelete:           4,
		ChangeNodeHistoryVersion:  5,
		ChangeRelHistoryVersion:   6,
		ChangeNodeHistoryTruncate: 7,
		ChangeRelHistoryTruncate:  8,
		ChangeMeta:                9,
		ChangeClear:               10,
	}
	for tag, want := range pinned {
		if byte(tag) != want {
			t.Errorf("%v has wire number %d, pinned at %d", tag, byte(tag), want)
		}
	}
}
