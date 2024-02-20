// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rate provides a rate limiter.
package rate

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit float64

// Inf is the infinite rate limit; it allows all events (even if burst is zero).
const Inf = Limit(math.MaxFloat64)

// Every converts a minimum time interval between events to a Limit.
// 特定时间间隔内允许发生的事件数，比如每1s允许发生10个事件，那么interval=100ms，则limit=10
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

// A Limiter controls how frequently events are allowed to happen.
// It implements a "token bucket" of size b, initially full and refilled
// at rate r tokens per second.
// Informally, in any large enough time interval, the Limiter limits the
// rate to r tokens per second, with a maximum burst size of b events.
// As a special case, if r == Inf (the infinite rate), b is ignored.
// See https://en.wikipedia.org/wiki/Token_bucket for more about token buckets.
//
// The zero value is a valid Limiter, but it will reject all events.
// Use NewLimiter to create non-zero Limiters.
//
// Limiter has three main methods, Allow, Reserve, and Wait.
// Most callers should use Wait.
//
// Each of the three methods consumes a single token.
// They differ in their behavior when no token is available.
// If no token is available, Allow returns false.
// If no token is available, Reserve returns a reservation for a future token
// and the amount of time the caller must wait before using it.
// If no token is available, Wait blocks until one can be obtained
// or its associated context.Context is canceled.
//
// The methods AllowN, ReserveN, and WaitN consume n tokens.
//
// Limiter is safe for simultaneous use by multiple goroutines.
type Limiter struct {
	// 锁
	mu sync.Mutex
	// 限速r，每秒允许limit个token
	limit Limit
	// 令牌桶的最大burst，即允许的突发token最大数量，允许突发流量的最大值
	burst int
	// 当前令牌桶中的可用token数量
	tokens float64
	// last is the last time the limiter's tokens field was updated
	// 最近一次更新令牌桶的时刻
	last time.Time
	// lastEvent is the latest time of a rate-limited event (past or future)
	// 最近一次事件发生的时间
	// last：表示最近一次申请token的时间，lastEvent表示最近一次事件发生的时间（获取到令牌的时间）
	lastEvent time.Time
}

// Limit returns the maximum overall event rate.
func (lim *Limiter) Limit() Limit {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.limit
}

// Burst returns the maximum burst size. Burst is the maximum number of tokens
// that can be consumed in a single call to Allow, Reserve, or Wait, so higher
// Burst values allow more events to happen at once.
// A zero Burst allows no events, unless limit == Inf.
func (lim *Limiter) Burst() int {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.burst
}

// TokensAt returns the number of tokens available at time t.
func (lim *Limiter) TokensAt(t time.Time) float64 {
	lim.mu.Lock()
	_, tokens := lim.advance(t) // does not mutate lim
	lim.mu.Unlock()
	return tokens
}

// Tokens returns the number of tokens available now.
func (lim *Limiter) Tokens() float64 {
	return lim.TokensAt(time.Now())
}

// NewLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewLimiter(r Limit, b int) *Limiter {
	return &Limiter{
		limit: r,
		burst: b,
	}
}

// Allow reports whether an event may happen now.
// 当前是否允许事件发生，如果返回true，则表示可以立即执行，否则不能执行
func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

// AllowN reports whether n events may happen at time t.
// Use this method if you intend to drop / skip events that exceed the rate limit.
// Otherwise use Reserve or Wait.
// 当前是否允许n个事件发生，如果返回true，则表示可以立即执行，否则不能执行
func (lim *Limiter) AllowN(t time.Time, n int) bool {
	return lim.reserveN(t, n, 0).ok
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
type Reservation struct {
	ok  bool
	lim *Limiter
	// 预订的tokens数量，也就是申请的数量
	tokens int
	// 可执行的时间，也就是满足申请数量的时间
	timeToAct time.Time
	// This is the Limit at reservation time, it can change later.
	// 由于limit可以动态调整，所以在reservation上保留一份当时申请时的数据备份
	limit Limit
}

// OK returns whether the limiter can provide the requested number of tokens
// within the maximum wait time.  If OK is false, Delay returns InfDuration, and
// Cancel does nothing.
func (r *Reservation) OK() bool {
	return r.ok
}

// Delay is shorthand for DelayFrom(time.Now()).
// 获取预订令牌需要延迟等待的时间
func (r *Reservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// InfDuration is the duration returned by Delay when a Reservation is not OK.
const InfDuration = time.Duration(math.MaxInt64)

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
// InfDuration means the limiter cannot grant the tokens requested in this
// Reservation within the maximum wait time.
// 根据r.timeToAct时间，计算出需要延迟的时间
func (r *Reservation) DelayFrom(t time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}
	delay := r.timeToAct.Sub(t)
	if delay < 0 {
		return 0
	}
	return delay
}

// Cancel is shorthand for CancelAt(time.Now()).
// 取消预订令牌，释放令牌
func (r *Reservation) Cancel() {
	r.CancelAt(time.Now())
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible,
// considering that other reservations may have already been made.
func (r *Reservation) CancelAt(t time.Time) {
	// 未获取到令牌，则直接返回
	if !r.ok {
		return
	}

	// 全过程加锁
	r.lim.mu.Lock()
	defer r.lim.mu.Unlock()

	// 无限速、预订tokens为0、预订时间小于当前时间，则直接返回
	// 为何预订时间小于当前时间，就直接返回呢？不需要释放资源吗？
	// 取消操作，仅会发生在ctx取消或超时情况下，此时r.timeToAct正常应该还未到达。
	// 如果已到达执行时间，说明已正常使用令牌，也就无需释放操作了
	if r.lim.limit == Inf || r.tokens == 0 || r.timeToAct.Before(t) {
		return
	}

	// calculate tokens to restore
	// The duration between lim.lastEvent and r.timeToAct tells us how many tokens were reserved
	// after r was obtained. These tokens should not be restored.
	// 计算需要恢复的令牌数量
	// 问题：TODO
	// 1、为何r.timeToAct和lim.lastEvent之差来计算token数？而不是直接把r.tokens直接返还？
	// 1.1 因为随着执行时间，已经有一些时间被消耗，那这段时间对应的token应该被剔除
	// 1.2 如果lastEvent未被其他请求刷新，那么r.limit.lastEent==r.timeToAct，此时直接恢复r.tokens即可
	// 1.3 如果lastEvent被其他请求刷新了，那么r.limit.lastEent>r.timeToAct，此时如何计算正确的退还tokens数量呢？
	// 2、这里r.lim.lastEvent是否会有并发操作问题，比如其他请求更新了lastEvent后？？
	restoreTokens := float64(r.tokens) - r.limit.tokensFromDuration(r.lim.lastEvent.Sub(r.timeToAct))
	fmt.Println("lim.lastEvent-r.timeToAck:", r.limit.tokensFromDuration(r.lim.lastEvent.Sub(r.timeToAct)))
	// t3-t2=1
	fmt.Println("restoreTokens:", restoreTokens) // 2-1=1
	if restoreTokens <= 0 {
		return
	}
	// advance time to now
	t, tokens := r.lim.advance(t)
	fmt.Println("advance tokens:", tokens) // -2
	// calculate new number of tokens
	// -2+1=-1
	tokens += restoreTokens
	if burst := float64(r.lim.burst); tokens > burst {
		tokens = burst
	}
	// update state
	r.lim.last = t
	r.lim.tokens = tokens
	fmt.Println("cancel Done:", r.lim.last, " r.lim.tokens:", r.lim.tokens)

	// 如果r是最后一个预约token
	if r.timeToAct == r.lim.lastEvent {
		// 时间回退-r.tokens对应的时间
		prevEvent := r.timeToAct.Add(r.limit.durationFromTokens(float64(-r.tokens)))
		if !prevEvent.Before(t) {
			r.lim.lastEvent = prevEvent
		}
	}

	// 非最后一个预约时，并不会更新lim.lastEvent
	// 也就是说，取消预约时，会释放token，但并不会改变已取的预约令牌的r.timeToAct和lim.lastEvent
}

// Reserve is shorthand for ReserveN(time.Now(), 1).
// 申请1个token，返回一个Reservation对象，该对象指示了等待多长时间后才能获得这个token
func (lim *Limiter) Reserve() *Reservation {
	return lim.ReserveN(time.Now(), 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events happen.
// The Limiter takes this Reservation into account when allowing future events.
// The returned Reservation’s OK() method returns false if n exceeds the Limiter's burst size.
// Usage example:
//
//	r := lim.ReserveN(time.Now(), 1)
//	if !r.OK() {
//	  // Not allowed to act! Did you remember to set lim.burst to be > 0 ?
//	  return
//	}
//	time.Sleep(r.Delay())
//	Act()
//
// Use this method if you wish to wait and slow down in accordance with the rate limit without dropping events.
// If you need to respect a deadline or cancel the delay, use Wait instead.
// To drop or skip events exceeding rate limit, use Allow instead.
// 申请n个token，返回一个Reservation对象，该对象指示了等待多长时间后才能获得这n个token
// maxFutureReserve参数使用InfDuration，表示可等待时间无限大，具体等待时间由n的数量决定
func (lim *Limiter) ReserveN(t time.Time, n int) *Reservation {
	r := lim.reserveN(t, n, InfDuration)
	return &r
}

// Wait is shorthand for WaitN(ctx, 1).
func (lim *Limiter) Wait(ctx context.Context) (err error) {
	return lim.WaitN(ctx, 1)
}

// WaitN blocks until lim permits n events to happen.
// It returns an error if n exceeds the Limiter's burst size, the Context is
// canceled, or the expected wait time exceeds the Context's Deadline.
// The burst limit is ignored if the rate limit is Inf.
// 阻塞请求直到获取到足够数量的tokens
// 下面集中情况返回error：
// 1. n > burst && limit != Inf ，当limit为Inf时，不校验burst，也就是可以申请任意数量的token
// 2. ctx被cancelled取消
// 3. 等待时间超过ctx的deadline
func (lim *Limiter) WaitN(ctx context.Context, n int) (err error) {
	// The test code calls lim.wait with a fake timer generator.
	// This is the real timer generator.
	newTimer := func(d time.Duration) (<-chan time.Time, func() bool, func()) {
		timer := time.NewTimer(d)
		return timer.C, timer.Stop, func() {}
	}
	// 可以看到，为了满足测试需要，代码编写上会做特殊处理，以方便支持测试

	return lim.wait(ctx, n, time.Now(), newTimer)
}

// wait is the internal implementation of WaitN.
// 从t时刻开始，等待获取总数为n的令牌数
// 第4个参数也太脏了
// newTimer func(d time.Duration) (<-chan time.Time, func() bool, func())
// 参数变量为newTimer，类型为func(d time.Duration) (<-chan time.Time, func() bool, func())
// 也就是说，func作为类型时，需要写明函数签名（入参和返回值）
func (lim *Limiter) wait(ctx context.Context, n int, t time.Time, newTimer func(d time.Duration) (<-chan time.Time, func() bool, func())) error {
	// 先加锁
	lim.mu.Lock()
	burst := lim.burst
	limit := lim.limit
	// 为何这么快就解锁了呢？？后面操作不需要锁吗？TODO
	lim.mu.Unlock()

	// 非无限limit时，超过burst直接返回error
	if n > burst && limit != Inf {
		return fmt.Errorf("rate: Wait(n=%d) exceeds limiter's burst %d", n, burst)
	}
	// Check if ctx is already cancelled
	// 校验一次是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// Determine wait limit
	// 计算可等待的最大时间，默认无限，如ctx的deadline存在，则取deadline减去当前时间计算出可等待时间
	waitLimit := InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(t)
	}
	// Reserve
	// 预订申请令牌
	r := lim.reserveN(t, n, waitLimit)
	// 只有超过waitLimit一种情况会返回!ok，也就是没有足够时间生成对应数量的令牌
	if !r.ok {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", n)
	}
	// Wait if necessary
	// 计算需要等待时间长度
	delay := r.DelayFrom(t)
	// 等待时间长度为0，则直接返回
	if delay == 0 {
		return nil
	}

	// 需要等待的情况
	ch, stop, advance := newTimer(delay)
	defer stop()
	advance() // only has an effect when testing
	select {
	// 计时器等待时间到，则返回nil，表示获取到了足够的令牌
	case <-ch:
		// We can proceed.
		return nil
	case <-ctx.Done():
		// ctx被取消，则返回错误
		// Context was canceled before we could proceed.  Cancel the
		// reservation, which may permit other events to proceed sooner.
		// 取消预订操作，释放令牌
		r.Cancel()
		return ctx.Err()
	}
}

// SetLimit is shorthand for SetLimitAt(time.Now(), newLimit).
func (lim *Limiter) SetLimit(newLimit Limit) {
	lim.SetLimitAt(time.Now(), newLimit)
}

// SetLimitAt sets a new Limit for the limiter. The new Limit, and Burst, may be violated
// or underutilized by those which reserved (using Reserve or Wait) but did not yet act
// before SetLimitAt was called.
func (lim *Limiter) SetLimitAt(t time.Time, newLimit Limit) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	t, tokens := lim.advance(t)

	lim.last = t
	lim.tokens = tokens
	lim.limit = newLimit
}

// SetBurst is shorthand for SetBurstAt(time.Now(), newBurst).
func (lim *Limiter) SetBurst(newBurst int) {
	lim.SetBurstAt(time.Now(), newBurst)
}

// SetBurstAt sets a new burst size for the limiter.
func (lim *Limiter) SetBurstAt(t time.Time, newBurst int) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	t, tokens := lim.advance(t)

	lim.last = t
	lim.tokens = tokens
	lim.burst = newBurst
}

// reserveN is a helper method for AllowN, ReserveN, and WaitN.
// maxFutureReserve specifies the maximum reservation wait duration allowed.
// reserveN returns Reservation, not *Reservation, to avoid allocation in AllowN and WaitN.
// maxFutureReserve几个入参的值：
// 1. 0，表示不等待，对应AllowN使用
// 2. InfDuration，表示不限等待时间，对应ReserveN使用
// 3. ctx超时时间，表示等待时间不能超过ctx的deadline时间，对应WaitN使用
func (lim *Limiter) reserveN(t time.Time, n int, maxFutureReserve time.Duration) Reservation {
	// 加锁
	lim.mu.Lock()
	defer lim.mu.Unlock()

	// limit = Inf，表示不限速
	if lim.limit == Inf {
		return Reservation{
			ok:        true, // 当前校验结果通过
			lim:       lim,
			tokens:    n,
			timeToAct: t,
		}
		// 限速为0，此时只看是否超过burst
		// 限速为0，具体作用是什么？？？TODO
		// 生产速度为0，但桶内可用burst仅为初始的burst个，用完就没了？？
	} else if lim.limit == 0 {
		var ok bool
		if lim.burst >= n {
			ok = true
			lim.burst -= n
		}
		return Reservation{
			ok:        ok,
			lim:       lim,
			tokens:    lim.burst,
			timeToAct: t,
		}
	}

	// 计算截止时间t时刻，更新lim可用tokens数量：桶中剩余+间隔时间内心生产token数（总数不超过桶容量burst）
	t, tokens := lim.advance(t)
	// fmt.Println(tokens, n)
	if n == 2 {
		fmt.Println("advance:", tokens)
	}

	// Calculate the remaining number of tokens resulting from the request.
	// 减去当前申请使用token数量n
	tokens -= float64(n)
	if n == 2 {
		fmt.Println("calc tokens:", tokens)
	}

	// Calculate the wait duration
	var waitDuration time.Duration
	// 结果为负值，则说明超过可用数量
	if tokens < 0 {
		// 计算需要等待的时间，超出数量-tokens个，利用durationFromTokens计算需等待时间
		waitDuration = lim.limit.durationFromTokens(-tokens)
	}
	if n == 2 {
		fmt.Println("waitDuration:", waitDuration) // 500ms
	}

	// Decide result
	// 申请量n未超过桶容量，且等待时间不超过最大允许等待时间（也就是说可用）
	ok := n <= lim.burst && waitDuration <= maxFutureReserve

	// Prepare reservation
	r := Reservation{
		ok:    ok,
		lim:   lim,
		limit: lim.limit,
	}
	if ok {
		// tokens到底表示什么？为什么把n赋值给tokens呢？
		// TODO
		r.tokens = n
		// 可执行的是啊金
		r.timeToAct = t.Add(waitDuration)

		// Update state
		// 更新上次处理时间，更新桶内令牌数量
		lim.last = t
		// 更新桶内令牌数量（桶内剩余/可用令牌数）
		lim.tokens = tokens
		// 更新最后一次事件的时间，也就是申请后最后可执行的时间
		lim.lastEvent = r.timeToAct
		// fmt.Println("-----get-----")
		// fmt.Printf("lim=%#v\n", lim)
		// fmt.Printf("r=%#v\n", r)
		fmt.Printf("r.tokens=%d, r.timeToAck=%s\n", r.tokens, r.timeToAct)
		fmt.Printf("lim.tokens=%f, lim.last=%s, lim.lastEvent=%s  \n", lim.tokens, lim.last, lim.lastEvent)
		fmt.Println("-------------------")
	}

	// 对于ok=false的情况，tokens和timeToAct都未赋值，因此返回的Reservation中这两个字段为零值，lim也不会更新

	return r
}

// advance calculates and returns an updated state for lim resulting from the passage of time.
// lim is not changed.
// advance requires that lim.mu is held.
// 计算并返回经过时间t后，lim的状态
func (lim *Limiter) advance(t time.Time) (newT time.Time, newTokens float64) {
	last := lim.last
	// fmt.Println(last)
	// 这里为何要这么处理？为了确保下面计算时间间隔时，结果为0。防止出现负数
	if t.Before(last) {
		last = t
	}

	// Calculate the new number of tokens, due to time that passed.
	// 计算经过时间t后，lim的tokens数量
	// 1. 计算距离上一次处理时的时间差
	// TODO：注意这个时间是last，而不是lastEvent
	elapsed := t.Sub(last)
	// 初始时last为空，则elapsed为maxDuration
	// fmt.Println(int64(elapsed) == (1<<63 - 1)) // true
	// 2. 根据时间差计算可生产的tokens数量，lazyload思想
	delta := lim.limit.tokensFromDuration(elapsed)
	// 3. 更新tokens数量，桶内原有令牌数+这段时间内可产生的新增令牌数
	tokens := lim.tokens + delta
	// 4. 计算新的tokens数量，确保不超过桶容量burst
	if burst := float64(lim.burst); tokens > burst {
		tokens = burst
	}
	// fmt.Println(tokens, lim.burst)
	return t, tokens
}

// durationFromTokens is a unit conversion function from the number of tokens to the duration
// of time it takes to accumulate them at a rate of limit tokens per second.
// 生产tokens数量的token需要多少时间
func (limit Limit) durationFromTokens(tokens float64) time.Duration {
	if limit <= 0 {
		return InfDuration
	}
	seconds := tokens / float64(limit)
	return time.Duration(float64(time.Second) * seconds)
}

// tokensFromDuration is a unit conversion function from a time duration to the number of tokens
// which could be accumulated during that duration at a rate of limit tokens per second.
// 给定时间间隔d，可以生产多少数量的token
func (limit Limit) tokensFromDuration(d time.Duration) float64 {
	if limit <= 0 {
		return 0
	}
	return d.Seconds() * float64(limit)
}
