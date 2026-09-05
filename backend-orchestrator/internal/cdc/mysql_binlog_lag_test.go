package cdc

import "testing"

func TestBinlogAbsPos(t *testing.T) {
	sizes := map[string]int64{
		"mysql-bin.000001": 1000,
		"mysql-bin.000002": 2000,
		"mysql-bin.000003": 500,
	}
	order := []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"}

	cases := []struct {
		name   string
		pos    BinlogPosition
		want   int64
		wantOK bool
	}{
		{"first file", BinlogPosition{File: "mysql-bin.000001", Pos: 100}, 100, true},
		{"second file adds preceding size", BinlogPosition{File: "mysql-bin.000002", Pos: 50}, 1050, true},
		{"third file adds two preceding", BinlogPosition{File: "mysql-bin.000003", Pos: 10}, 3010, true},
		{"unknown/purged file", BinlogPosition{File: "mysql-bin.000099", Pos: 10}, 0, false},
	}
	for _, c := range cases {
		got, ok := binlogAbsPos(sizes, order, c.pos)
		if ok != c.wantOK || (ok && got != c.want) {
			t.Errorf("%s: binlogAbsPos = (%d,%v), want (%d,%v)", c.name, got, ok, c.want, c.wantOK)
		}
	}
}

func TestBinlogLagFromIndex(t *testing.T) {
	sizes := map[string]int64{
		"mysql-bin.000001": 1000,
		"mysql-bin.000002": 2000,
		"mysql-bin.000003": 500,
	}
	order := []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"}

	t.Run("same file", func(t *testing.T) {
		lag, err := binlogLagFromIndex(sizes, order,
			BinlogPosition{File: "mysql-bin.000002", Pos: 1500}, // current
			BinlogPosition{File: "mysql-bin.000002", Pos: 500})  // committed
		if err != nil {
			t.Fatal(err)
		}
		if lag != 1000 {
			t.Errorf("lag = %d, want 1000", lag)
		}
	})

	t.Run("across files", func(t *testing.T) {
		// current = file3 pos 100 -> abs 3100; committed = file1 pos 200 -> abs 200
		lag, err := binlogLagFromIndex(sizes, order,
			BinlogPosition{File: "mysql-bin.000003", Pos: 100},
			BinlogPosition{File: "mysql-bin.000001", Pos: 200})
		if err != nil {
			t.Fatal(err)
		}
		if lag != 2900 {
			t.Errorf("lag = %d, want 2900", lag)
		}
	})

	t.Run("caught up -> zero", func(t *testing.T) {
		p := BinlogPosition{File: "mysql-bin.000003", Pos: 100}
		lag, err := binlogLagFromIndex(sizes, order, p, p)
		if err != nil {
			t.Fatal(err)
		}
		if lag != 0 {
			t.Errorf("lag = %d, want 0", lag)
		}
	})

	t.Run("committed ahead clamps to zero", func(t *testing.T) {
		lag, err := binlogLagFromIndex(sizes, order,
			BinlogPosition{File: "mysql-bin.000001", Pos: 100}, // current behind
			BinlogPosition{File: "mysql-bin.000002", Pos: 100}) // committed ahead
		if err != nil {
			t.Fatal(err)
		}
		if lag != 0 {
			t.Errorf("lag = %d, want 0 (clamped)", lag)
		}
	})

	t.Run("purged committed file errors", func(t *testing.T) {
		_, err := binlogLagFromIndex(sizes, order,
			BinlogPosition{File: "mysql-bin.000003", Pos: 100},
			BinlogPosition{File: "mysql-bin.000000", Pos: 0}) // purged
		if err == nil {
			t.Error("expected error for purged committed file, got nil")
		}
	})
}
