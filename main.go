// pooldepth — replicate Dune query 7685210 (Aerodrome Slipstream hourly pool depth)
// directly from a Base JSON-RPC endpoint. Stdlib only, no go-ethereum.
//
// Output columns (CSV, same as the Dune query):
//
//	hour, open, high, low, close, impact_p90, tier,
//	base_250, base_500, base_1500, quote_250, quote_500, quote_1500
//
// Usage:
//
//	go run . -rpc https://mainnet.base.org \
//	         -pool 0x0aed2bd5abdffcde57c0bcf30e75cd594b8876a9 \
//	         -quote token1 -start 2026-05-01 -out depth.csv
//
// Semantics match the SQL:
//   - swaps with liquidity == 0 are ignored
//   - ohlc / impact / state are per swap-hour (hours with no swaps are absent)
//   - a mint/burn counts toward hour H if event_time <= close_ts(H) (last swap in H);
//     events after the last swap-hour are dropped
//   - only mints/burns at or after -start are included (positions minted earlier are NOT
//     in the depth — this is how the Dune query behaves too)
//   - impact_p90 is an exact percentile here; Dune uses approx_percentile (t-digest),
//     so expect tiny differences on that column only
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------- event topics (UniswapV3 ABI, Slipstream uses the same)
const (
	topicSwap = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" // Swap(address,address,int256,int256,uint160,uint128,int24)
	topicMint = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde" // Mint(address,address,int24,int24,uint128,uint256,uint256)
	topicBurn = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c" // Burn(address,int24,int24,uint128,uint256,uint256)

	selToken0   = "0x0dfe1681"
	selToken1   = "0xd21220a7"
	selDecimals = "0x313ce567"
)

// ---------------------------------------------------------------- minimal JSON-RPC client
type rpcClient struct {
	url  string
	http *http.Client
	mu   sync.Mutex
	id   int64
}

type rpcReq struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int64         `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcResp struct {
	ID     int64           `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func newRPC(url string) *rpcClient {
	return &rpcClient{url: url, http: &http.Client{Timeout: 90 * time.Second}}
}

func (c *rpcClient) nextID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id++
	return c.id
}

func (c *rpcClient) post(body interface{}) ([]byte, error) {
	b, _ := json.Marshal(body)
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		resp, err := c.http.Post(c.url, "application/json", bytes.NewReader(b))
		if err == nil {
			out, rerr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if rerr == nil && resp.StatusCode == 200 {
				return out, nil
			}
			lastErr = fmt.Errorf("http %d: %s", resp.StatusCode, string(out))
			// 4xx other than 429/408 will not get better on retry (e.g. 413 "payload too large"
			// on a wide getLogs range) — return now so the caller can bisect immediately
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 && resp.StatusCode != 408 {
				return nil, lastErr
			}
		} else {
			lastErr = err
		}
		time.Sleep(time.Duration(500*(1<<attempt)) * time.Millisecond)
	}
	return nil, lastErr
}

func (c *rpcClient) call(method string, params ...interface{}) (json.RawMessage, error) {
	out, err := c.post(rpcReq{JSONRPC: "2.0", ID: c.nextID(), Method: method, Params: params})
	if err != nil {
		return nil, err
	}
	var r rpcResp
	if err := json.Unmarshal(out, &r); err != nil {
		return nil, fmt.Errorf("%s: bad json: %v", method, err)
	}
	if r.Error != nil {
		return nil, fmt.Errorf("%s: rpc error %d: %s", method, r.Error.Code, r.Error.Message)
	}
	return r.Result, nil
}

// batch: several calls in one HTTP round-trip
func (c *rpcClient) batch(reqs []rpcReq) (map[int64]json.RawMessage, error) {
	out, err := c.post(reqs)
	if err != nil {
		return nil, err
	}
	var rs []rpcResp
	if err := json.Unmarshal(out, &rs); err != nil {
		// some providers answer a rejected batch with a single error object
		var one rpcResp
		if json.Unmarshal(out, &one) == nil && one.Error != nil {
			return nil, fmt.Errorf("batch of %d rejected: %s (lower -batch)", len(reqs), one.Error.Message)
		}
		return nil, fmt.Errorf("batch: bad json: %v", err)
	}
	res := make(map[int64]json.RawMessage, len(rs))
	for _, r := range rs {
		if r.Error != nil {
			return nil, fmt.Errorf("batch item %d: %s", r.ID, r.Error.Message)
		}
		res[r.ID] = r.Result
	}
	return res, nil
}

// ---------------------------------------------------------------- hex helpers
func hexToBig(s string) *big.Int {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return new(big.Int)
	}
	n, ok := new(big.Int).SetString(s, 16)
	if !ok {
		log.Fatalf("bad hex: %s", s)
	}
	return n
}

func hexToUint64(s string) uint64 { return hexToBig(s).Uint64() }

// word i of ABI-encoded data (32 bytes each)
func word(data string, i int) string {
	d := strings.TrimPrefix(data, "0x")
	return d[i*64 : (i+1)*64]
}

// int24 stored as a 32-byte two's-complement word (topic or data)
func wordToInt24(w string) int64 {
	n := hexToBig(w)
	if n.Bit(255) == 1 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return n.Int64()
}

func bigToFloat(n *big.Int) float64 {
	f, _ := new(big.Float).SetInt(n).Float64()
	return f
}

// ---------------------------------------------------------------- chain access
type ethLog struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	LogIndex    string   `json:"logIndex"`
}

func (c *rpcClient) blockNumber() uint64 {
	r, err := c.call("eth_blockNumber")
	if err != nil {
		log.Fatal(err)
	}
	var s string
	json.Unmarshal(r, &s)
	return hexToUint64(s)
}

func (c *rpcClient) blockTimestamp(n uint64) uint64 {
	r, err := c.call("eth_getBlockByNumber", fmt.Sprintf("0x%x", n), false)
	if err != nil {
		log.Fatal(err)
	}
	var b struct {
		Timestamp string `json:"timestamp"`
	}
	json.Unmarshal(r, &b)
	return hexToUint64(b.Timestamp)
}

// first block with timestamp >= ts
func (c *rpcClient) blockAtOrAfter(ts uint64) uint64 {
	lo, hi := uint64(0), c.blockNumber()
	for lo < hi {
		mid := (lo + hi) / 2
		if c.blockTimestamp(mid) >= ts {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (c *rpcClient) ethCall(to, data string) string {
	r, err := c.call("eth_call", map[string]string{"to": to, "data": data}, "latest")
	if err != nil {
		log.Fatal(err)
	}
	var s string
	json.Unmarshal(r, &s)
	return s
}

// getLogs over [from,to] for any of the given topic0s (one request per chunk, OR-filtered),
// splitting the range on provider errors
func (c *rpcClient) getLogs(addr string, topic0s []string, from, to, step uint64, workers int) []ethLog {
	type chunk struct{ from, to uint64 }
	var chunks []chunk
	for f := from; f <= to; f += step {
		t := f + step - 1
		if t > to {
			t = to
		}
		chunks = append(chunks, chunk{f, t})
	}
	results := make([][]ethLog, len(chunks))
	idx := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	done, total := 0, 0
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				results[i] = c.getLogsRange(addr, topic0s, chunks[i].from, chunks[i].to)
				mu.Lock()
				done++
				total += len(results[i])
				log.Printf("getLogs chunk %d/%d (%d..%d): %d logs so far", done, len(chunks), chunks[i].from, chunks[i].to, total)
				mu.Unlock()
			}
		}()
	}
	for i := range chunks {
		idx <- i
	}
	close(idx)
	wg.Wait()
	var all []ethLog // chunk order is preserved; swaps are re-sorted by (block, logIndex) later anyway
	for _, r := range results {
		all = append(all, r...)
	}
	return all
}

// provider throttling — retry, never bisect on these
func isRateLimit(err error) bool {
	s := strings.ToLower(err.Error())
	for _, k := range []string{"429", "rate limit", "over rate", "too many requests", "capacity", "compute units", "throughput"} {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

func (c *rpcClient) getLogsRange(addr string, topic0s []string, from, to uint64) []ethLog {
	var r json.RawMessage
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		r, err = c.call("eth_getLogs", map[string]interface{}{
			"address":   addr,
			"topics":    []interface{}{topic0s},
			"fromBlock": fmt.Sprintf("0x%x", from),
			"toBlock":   fmt.Sprintf("0x%x", to),
		})
		if err == nil || !isRateLimit(err) {
			break
		}
		log.Printf("getLogs %d..%d throttled, retry %d: %v", from, to, attempt+1, err)
		time.Sleep(time.Duration(1000*(1<<uint(attempt))) * time.Millisecond)
	}
	if err != nil {
		if from == to {
			log.Fatal(err)
		}
		// most providers error on "too many results" — bisect
		mid := (from + to) / 2
		return append(c.getLogsRange(addr, topic0s, from, mid), c.getLogsRange(addr, topic0s, mid+1, to)...)
	}
	var logs []ethLog
	if err := json.Unmarshal(r, &logs); err != nil {
		log.Fatal(err)
	}
	return logs
}

// timestamps for a set of blocks, batched + concurrent
func (c *rpcClient) blockTimestamps(blocks []uint64, workers, batchSize int) map[uint64]uint64 {
	out := make(map[uint64]uint64, len(blocks))
	done := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	ch := make(chan []uint64)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range ch {
				reqs := make([]rpcReq, 0, len(chunk))
				idToBlock := make(map[int64]uint64, len(chunk))
				for _, b := range chunk {
					id := c.nextID()
					idToBlock[id] = b
					reqs = append(reqs, rpcReq{JSONRPC: "2.0", ID: id, Method: "eth_getBlockByNumber",
						Params: []interface{}{fmt.Sprintf("0x%x", b), false}})
				}
				var res map[int64]json.RawMessage
				var err error
				for attempt := 0; ; attempt++ {
					res, err = c.batch(reqs)
					if err == nil {
						break
					}
					if attempt >= 8 {
						log.Fatal(err)
					}
					log.Printf("batch retry %d: %v", attempt+1, err)
					time.Sleep(time.Duration(500*(1<<attempt)) * time.Millisecond)
				}
				mu.Lock()
				done += len(chunk)
				if done%1000 < len(chunk) {
					log.Printf("timestamps: %d/%d", done, len(blocks))
				}
				for id, raw := range res {
					var blk struct {
						Timestamp string `json:"timestamp"`
					}
					json.Unmarshal(raw, &blk)
					out[idToBlock[id]] = hexToUint64(blk.Timestamp)
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < len(blocks); i += batchSize {
		j := i + batchSize
		if j > len(blocks) {
			j = len(blocks)
		}
		ch <- blocks[i:j]
	}
	close(ch)
	wg.Wait()
	return out
}

// ---------------------------------------------------------------- domain types
type swap struct {
	block, logIdx, ts uint64
	sqrtp, l          float64
}

type liqEvent struct {
	ts   uint64
	tick int64
	net  float64
}

type hourRow struct {
	hour                   int64 // unix seconds, truncated to hour
	open, high, low, close float64
	impacts                []float64
	closeTs                uint64
	sp                     float64
}

type band struct {
	label string
	delta float64
}

var bands = []band{{"250", 0.025}, {"500", 0.050}, {"1500", 0.150}}

// ---------------------------------------------------------------- main
func main() {
	rpcURL := flag.String("rpc", "https://mainnet.base.org", "Base JSON-RPC endpoint")
	pool := flag.String("pool", "0x0aed2bd5abdffcde57c0bcf30e75cd594b8876a9", "CLPool address")
	quoteSlot := flag.String("quote", "token1", "slot holding the USD token: token0 | token1")
	startStr := flag.String("start", "2026-05-01", "start date (UTC), inclusive")
	endBlockFlag := flag.Uint64("end-block", 0, "end block (0 = latest)")
	step := flag.Uint64("step", 2000, "eth_getLogs block range per request (public Base RPC caps at 2000)")
	workers := flag.Int("workers", 4, "concurrent getLogs / header fetchers (public Base RPC: keep ≤4)")
	batchSize := flag.Int("batch", 100, "eth_getBlockByNumber calls per JSON-RPC batch (public Base RPC allows 10)")
	linearTS := flag.Bool("linear-ts", false, "derive timestamps as ts(first)+2s*(n-first) (Base/OP-stack fixed 2s blocks); verified against the last block")
	interval := flag.Int("interval", 60, "bucket size in minutes (60 = hourly like the Dune query)")
	outPath := flag.String("out", "depth.csv", "output CSV path")
	flag.Parse()

	if *interval <= 0 || 1440%*interval != 0 {
		log.Fatalf("-interval must be a positive divisor of 1440 minutes, got %d", *interval)
	}
	bucket := int64(*interval) * 60
	quoteIsToken0 := strings.ToLower(*quoteSlot) == "token0"
	startT, err := time.Parse("2006-01-02", *startStr)
	if err != nil {
		log.Fatal(err)
	}
	rpc := newRPC(*rpcURL)
	poolAddr := strings.ToLower(*pool)

	// --- token metadata (token0/token1/decimals) — replaces clpool_call_initialize + tokens.erc20
	token0 := "0x" + word(rpc.ethCall(poolAddr, selToken0), 0)[24:]
	token1 := "0x" + word(rpc.ethCall(poolAddr, selToken1), 0)[24:]
	dec0 := float64(hexToUint64(rpc.ethCall(token0, selDecimals)))
	dec1 := float64(hexToUint64(rpc.ethCall(token1, selDecimals)))
	quoteDec := dec1
	if quoteIsToken0 {
		quoteDec = dec0
	}
	log.Printf("token0=%s (%.0f dec) token1=%s (%.0f dec) quote=%s", token0, dec0, token1, dec1, *quoteSlot)

	// --- block range
	fromBlock := rpc.blockAtOrAfter(uint64(startT.Unix()))
	toBlock := *endBlockFlag
	if toBlock == 0 {
		toBlock = rpc.blockNumber()
	}
	log.Printf("blocks %d..%d", fromBlock, toBlock)

	// --- logs
	var swapLogs, mintLogs, burnLogs []ethLog
	for _, l := range rpc.getLogs(poolAddr, []string{topicSwap, topicMint, topicBurn}, fromBlock, toBlock, *step, *workers) {
		switch l.Topics[0] {
		case topicSwap:
			swapLogs = append(swapLogs, l)
		case topicMint:
			mintLogs = append(mintLogs, l)
		case topicBurn:
			burnLogs = append(burnLogs, l)
		}
	}
	log.Printf("logs: %d swaps, %d mints, %d burns", len(swapLogs), len(mintLogs), len(burnLogs))

	// --- timestamps for every block touched
	blockSet := map[uint64]struct{}{}
	for _, l := range swapLogs {
		blockSet[hexToUint64(l.BlockNumber)] = struct{}{}
	}
	for _, l := range mintLogs {
		blockSet[hexToUint64(l.BlockNumber)] = struct{}{}
	}
	for _, l := range burnLogs {
		blockSet[hexToUint64(l.BlockNumber)] = struct{}{}
	}
	blocks := make([]uint64, 0, len(blockSet))
	for b := range blockSet {
		blocks = append(blocks, b)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	log.Printf("fetching %d block timestamps", len(blocks))
	var tsOf map[uint64]uint64
	if *linearTS && len(blocks) > 0 {
		first, last := blocks[0], blocks[len(blocks)-1]
		t0, t1 := rpc.blockTimestamp(first), rpc.blockTimestamp(last)
		if t1-t0 != 2*(last-first) {
			log.Fatalf("-linear-ts: blocks %d..%d span %ds, expected %ds — chain is not 2s/block, drop the flag", first, last, t1-t0, 2*(last-first))
		}
		tsOf = make(map[uint64]uint64, len(blocks))
		for _, b := range blocks {
			tsOf[b] = t0 + 2*(b-first)
		}
		log.Printf("timestamps derived linearly from block %d (ts %d), verified at block %d", first, t0, last)
	} else {
		tsOf = rpc.blockTimestamps(blocks, *workers, *batchSize)
	}

	// --- decode swaps  (swap_samples)
	var swaps []swap
	for _, l := range swapLogs {
		liq := hexToBig(word(l.Data, 3)) // data: amount0, amount1, sqrtPriceX96, liquidity, tick
		if liq.Sign() == 0 {
			continue
		}
		sqrtPX96 := hexToBig(word(l.Data, 2))
		b := hexToUint64(l.BlockNumber)
		swaps = append(swaps, swap{
			block:  b,
			logIdx: hexToUint64(l.LogIndex),
			ts:     tsOf[b],
			sqrtp:  bigToFloat(sqrtPX96) / math.Pow(2.0, 96),
			l:      bigToFloat(liq),
		})
	}
	sort.Slice(swaps, func(i, j int) bool {
		if swaps[i].block != swaps[j].block {
			return swaps[i].block < swaps[j].block
		}
		return swaps[i].logIdx < swaps[j].logIdx
	})

	// --- priced + hourly (ohlc / impact / close state)  in one pass
	hourIdx := map[int64]*hourRow{}
	var hours []*hourRow
	for _, s := range swaps {
		var price, x float64
		if quoteIsToken0 {
			price = math.Pow(10.0, dec1-dec0) / (s.sqrtp * s.sqrtp)
			x = 1e4 * math.Pow(10.0, quoteDec) * s.sqrtp / s.l
		} else {
			price = (s.sqrtp * s.sqrtp) * math.Pow(10.0, dec0-dec1)
			x = 1e4 * math.Pow(10.0, quoteDec) * (1 / s.sqrtp) / s.l
		}
		impact := math.Min(math.Max(1-math.Pow(1/(1+x), 2), math.Pow(1+x, 2)-1), 1.0)

		h := int64(s.ts) / bucket * bucket
		row, ok := hourIdx[h]
		if !ok {
			row = &hourRow{hour: h, open: price, high: price, low: price}
			hourIdx[h] = row
			hours = append(hours, row)
		}
		row.high = math.Max(row.high, price)
		row.low = math.Min(row.low, price)
		row.close = price
		row.closeTs = s.ts
		row.sp = s.sqrtp
		row.impacts = append(row.impacts, impact)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].hour < hours[j].hour })
	log.Printf("%d swap buckets of %d min", len(hours), *interval)

	// --- liquidity delta events  (liq_delta_events)
	var events []liqEvent
	for _, l := range mintLogs {
		// topics: sig, owner, tickLower, tickUpper ; data: sender, amount, amount0, amount1
		lo, hi := wordToInt24(l.Topics[2]), wordToInt24(l.Topics[3])
		amt := bigToFloat(hexToBig(word(l.Data, 1)))
		ts := tsOf[hexToUint64(l.BlockNumber)]
		events = append(events, liqEvent{ts, lo, amt}, liqEvent{ts, hi, -amt})
	}
	for _, l := range burnLogs {
		// topics: sig, owner, tickLower, tickUpper ; data: amount, amount0, amount1
		lo, hi := wordToInt24(l.Topics[2]), wordToInt24(l.Topics[3])
		amt := bigToFloat(hexToBig(word(l.Data, 0)))
		ts := tsOf[hexToUint64(l.BlockNumber)]
		events = append(events, liqEvent{ts, lo, -amt}, liqEvent{ts, hi, amt})
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].ts < events[j].ts })

	// --- assign each event to the first swap-hour with closeTs >= event ts, then running sum per tick
	// liqNet[tick] is carried forward across hours; snapshot per hour after applying that hour's events.
	liqNet := map[int64]float64{}
	ei := 0
	out := csv.NewWriter(mustCreate(*outPath))
	defer out.Flush()
	out.Write([]string{"hour", "open", "high", "low", "close", "impact_p90", "tier",
		"base_250", "base_500", "base_1500", "quote_250", "quote_500", "quote_1500"})

	rows := 0
	for _, h := range hours {
		for ei < len(events) && events[ei].ts <= h.closeTs {
			liqNet[events[ei].tick] += events[ei].net
			ei++
		}

		// seg / seg_px: sorted ticks, cumulative active liquidity, segment [s_lo, s_hi)
		ticks := make([]int64, 0, len(liqNet))
		for t := range liqNet {
			ticks = append(ticks, t)
		}
		sort.Slice(ticks, func(i, j int) bool { return ticks[i] < ticks[j] })

		amt0 := make([]float64, len(bands))
		amt1 := make([]float64, len(bands))
		active := 0.0
		for i := 0; i+1 < len(ticks); i++ {
			active += liqNet[ticks[i]]
			if active <= 0 {
				continue
			}
			sLo := math.Pow(1.0001, float64(ticks[i])/2.0)
			sHi := math.Pow(1.0001, float64(ticks[i+1])/2.0)
			for bi, b := range bands {
				up := h.sp * math.Sqrt(1+b.delta)
				if sHi > h.sp && sLo < up {
					amt0[bi] += active * (1/math.Max(sLo, h.sp) - 1/math.Min(sHi, up))
				}
				dn := h.sp * math.Sqrt(1-b.delta)
				if sLo < h.sp && sHi > dn {
					amt1[bi] += active * (math.Min(sHi, h.sp) - math.Max(sLo, dn))
				}
			}
		}
		if len(ticks) < 2 {
			continue // no segments → no band_depths row → dropped by the inner join, same as SQL
		}

		// band_valued
		quote := make([]float64, len(bands))
		base := make([]float64, len(bands))
		for bi := range bands {
			if quoteIsToken0 {
				quote[bi] = amt0[bi] / math.Pow(10.0, dec0)
				base[bi] = -(amt1[bi] / (h.sp * h.sp) / math.Pow(10.0, dec0))
			} else {
				quote[bi] = amt1[bi] / math.Pow(10.0, dec1)
				base[bi] = -(amt0[bi] * (h.sp * h.sp) / math.Pow(10.0, dec1))
			}
		}

		p90 := percentile(h.impacts, 0.9)
		tier := "Reliable"
		switch {
		case p90 > 0.25:
			tier = "Ghost"
		case p90 >= 0.10:
			tier = "Very Thin"
		case p90 >= 0.03:
			tier = "Thin"
		case p90 >= 0.01:
			tier = "Moderate"
		}

		out.Write([]string{
			time.Unix(h.hour, 0).UTC().Format("2006-01-02 15:04:05"),
			f(h.open), f(h.high), f(h.low), f(h.close), f(p90), tier,
			f(base[0]), f(base[1]), f(base[2]),
			f(quote[0]), f(quote[1]), f(quote[2]),
		})
		rows++
	}
	log.Printf("wrote %d rows to %s", rows, *outPath)
}

// exact p-th percentile (linear interpolation). Dune's approx_percentile will differ slightly.
func percentile(v []float64, p float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	pos := p * float64(len(s)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return s[lo]
	}
	return s[lo] + (s[hi]-s[lo])*(pos-float64(lo))
}

func f(x float64) string { return strconv.FormatFloat(x, 'g', -1, 64) }

func mustCreate(p string) *os.File {
	fh, err := os.Create(p)
	if err != nil {
		log.Fatal(err)
	}
	return fh
}
