package oss

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

const (
	BwTokenBucketSlotRx int = iota
	BwTokenBucketSlotTx
	BwTokenBucketSlots
)

type BwTokenBucket struct {
	// Byte/S
	Bandwidth int64
	Limiter   *rate.Limiter
}

type BwTokenBuckets [BwTokenBucketSlots]*BwTokenBucket

func newBwTokenBucket(bandwidth int64) *BwTokenBucket {
	return &BwTokenBucket{
		Bandwidth: bandwidth,
		Limiter:   newEmptyTokenBucket(bandwidth),
	}
}

func newEmptyTokenBucket(bandwidth int64) *rate.Limiter {
	const defaultMaxBurstSize = 4 * 1024 * 1024
	maxBurstSize := (bandwidth * defaultMaxBurstSize) / (256 * 1024 * 1024)
	if maxBurstSize < defaultMaxBurstSize {
		maxBurstSize = defaultMaxBurstSize
	}
	tb := rate.NewLimiter(rate.Limit(bandwidth), int(maxBurstSize))
	tb.AllowN(time.Now(), int(maxBurstSize))
	return tb
}

func (tb *BwTokenBucket) LimitBandwidth(n int) {
	tb.Limiter.WaitN(context.Background(), n)
}
