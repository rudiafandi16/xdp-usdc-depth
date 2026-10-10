// Incremental mode (-state DIR): persist what later runs need so each run only fetches new blocks.
//
// The depth of a bucket depends on liqNet[tick] = running sum of every mint/burn with
// ts <= the bucket's close_ts. Every event before next_bucket is applied before any later bucket,
// so those are folded into DIR/liq_snapshot.csv (one float per tick, applied in the same order as a
// full scan, so the sums are bit-identical). Only events at/after next_bucket are kept raw, in
// DIR/liq_events.csv. Swaps are not kept: finished buckets are taken verbatim from the previous
// output CSV, and buckets from next_bucket onward are recomputed from logs re-fetched starting at
// next_block (the first block of that bucket). Output is byte-identical to a full scan with the
// same end block (on the same CPU architecture).
package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type runState struct {
	Pool       string `json:"pool"`
	Quote      string `json:"quote"`
	Start      string `json:"start"`
	Interval   int    `json:"interval"`
	NextBlock  uint64 `json:"next_block"`  // next run fetches logs from this block
	NextBucket int64  `json:"next_bucket"` // buckets >= this (unix s) are recomputed next run
	ScannedTo  uint64 `json:"scanned_to"`  // last block included in this run
	UpdatedAt  string `json:"updated_at"`
}

// one decoded Mint or Burn
type liqRec struct {
	block, logIdx, ts    uint64
	burn                 bool
	tickLower, tickUpper int64
	amount               *big.Int // uint128 liquidity amount, unsigned
}

func statePaths(dir string) (st, snap, events string) {
	return filepath.Join(dir, "state.json"), filepath.Join(dir, "liq_snapshot.csv"), filepath.Join(dir, "liq_events.csv")
}

// loadState returns nil state when DIR has none yet (first run = full scan from -start)
func loadState(dir string) (*runState, map[int64]float64, []liqRec) {
	sp, np, ep := statePaths(dir)
	b, err := os.ReadFile(sp)
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		log.Fatal(err)
	}
	var st runState
	if err := json.Unmarshal(b, &st); err != nil {
		log.Fatalf("%s: %v", sp, err)
	}
	snap := map[int64]float64{}
	for _, r := range readCSV(np) { // tick,net
		v, err := strconv.ParseFloat(r[1], 64)
		if err != nil {
			log.Fatalf("%s: %v", np, err)
		}
		snap[mustI(r[0])] = v
	}
	var recs []liqRec
	for _, r := range readCSV(ep) { // block,log_index,ts,kind,tick_lower,tick_upper,amount
		amt, ok := new(big.Int).SetString(r[6], 10)
		if !ok {
			log.Fatalf("%s: bad amount %q", ep, r[6])
		}
		recs = append(recs, liqRec{
			block: mustU(r[0]), logIdx: mustU(r[1]), ts: mustU(r[2]), burn: r[3] == "burn",
			tickLower: mustI(r[4]), tickUpper: mustI(r[5]), amount: amt,
		})
	}
	return &st, snap, recs
}

func saveState(dir string, st *runState, snap map[int64]float64, pending []liqRec) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	sp, np, ep := statePaths(dir)
	ticks := make([]int64, 0, len(snap))
	for t := range snap {
		ticks = append(ticks, t)
	}
	sort.Slice(ticks, func(i, j int) bool { return ticks[i] < ticks[j] })
	rows := [][]string{{"tick", "net"}}
	for _, t := range ticks {
		rows = append(rows, []string{strconv.FormatInt(t, 10), f(snap[t])}) // 'g' -1 round-trips exactly
	}
	writeCSV(np, rows)
	rows = [][]string{{"block", "log_index", "ts", "kind", "tick_lower", "tick_upper", "amount"}}
	for _, r := range pending {
		kind := "mint"
		if r.burn {
			kind = "burn"
		}
		rows = append(rows, []string{u(r.block), u(r.logIdx), u(r.ts), kind,
			strconv.FormatInt(r.tickLower, 10), strconv.FormatInt(r.tickUpper, 10), r.amount.String()})
	}
	writeCSV(ep, rows)
	st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b, _ := json.MarshalIndent(st, "", "  ")
	writeAtomic(sp, func(fh *os.File) { fh.Write(append(b, '\n')) })
}

// mergeLiq: stored + freshly fetched records, deduped on (block, logIndex), sorted by it
func mergeLiq(stored, fresh []liqRec) []liqRec {
	type key struct{ b, i uint64 }
	seen := make(map[key]bool, len(stored)+len(fresh))
	var out []liqRec
	for _, r := range append(append([]liqRec(nil), stored...), fresh...) {
		k := key{r.block, r.logIdx}
		if !seen[k] {
			seen[k] = true
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].block != out[j].block {
			return out[i].block < out[j].block
		}
		return out[i].logIdx < out[j].logIdx
	})
	return out
}

// finished rows of the previous output (hour < before), kept verbatim
func keptRows(path string, before int64) [][]string {
	var out [][]string
	for _, r := range readCSV(path) {
		t, err := time.Parse("2006-01-02 15:04:05", r[0])
		if err != nil {
			log.Fatalf("%s: bad hour %q", path, r[0])
		}
		if t.Unix() < before {
			out = append(out, r)
		}
	}
	return out
}

// rows after the header
func readCSV(path string) [][]string {
	fh, err := os.Open(path)
	if err != nil {
		log.Fatalf("incremental state is incomplete: %v", err)
	}
	defer fh.Close()
	rows, err := csv.NewReader(fh).ReadAll()
	if err != nil {
		log.Fatalf("%s: %v", path, err)
	}
	return rows[1:]
}

func writeCSV(path string, rows [][]string) {
	writeAtomic(path, func(fh *os.File) {
		w := csv.NewWriter(fh)
		w.WriteAll(rows)
		if err := w.Error(); err != nil {
			log.Fatal(err)
		}
	})
}

func writeAtomic(path string, fill func(*os.File)) {
	tmp := path + ".tmp"
	fh, err := os.Create(tmp)
	if err != nil {
		log.Fatal(err)
	}
	fill(fh)
	if err := fh.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Fatal(err)
	}
}

func u(x uint64) string { return strconv.FormatUint(x, 10) }

func mustU(s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Fatal(fmt.Errorf("bad uint %q: %v", s, err))
	}
	return n
}

func mustI(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Fatal(fmt.Errorf("bad int %q: %v", s, err))
	}
	return n
}
