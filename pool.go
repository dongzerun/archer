package archer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dongzerun/archer/ratelimit"
)

var (
	errClosed      = errors.New("redis: client is closed")
	errPoolTimeout = errors.New("redis: connection pool timeout")
)

type Conn interface {
	LastUsed() time.Time
	SetLastUsed(time.Time)
	Close() error
	ID() string
	Discard() error
}

type pool interface {
	First() Conn
	Get() (Conn, error)
	Put(Conn) error
	Remove(Conn) error
	Len() int
	FreeLen() int
	Close() error
}

type Options struct {
	// The network type, either tcp or unix.
	// Default is tcp4.
	Network string
	// host:port address.
	Addr string

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func() (Conn, error)

	// An optional password. Must match the password specified in the
	// requirepass server configuration option.
	Password string
	// A database to be selected after connecting to server.
	DB int64

	// The maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int

	// Sets the deadline for establishing new connections. If reached,
	// dial will fail with a timeout.
	DialTimeout time.Duration
	// Sets the deadline for socket reads. If reached, commands will
	// fail with a timeout instead of blocking.
	ReadTimeout time.Duration
	// Sets the deadline for socket writes. If reached, commands will
	// fail with a timeout instead of blocking.
	WriteTimeout time.Duration

	// The maximum number of socket connections.
	// Default is 10 connections.
	PoolSize int
	// Specifies amount of time client waits for connection if all
	// connections are busy before returning an error.
	// Default is 5 seconds.
	PoolTimeout time.Duration
	// Specifies amount of time after which client closes idle
	// connections. Should be less than server's timeout.
	// Default is to not close idle connections.
	IdleTimeout time.Duration
}

func (opt *Options) getNetwork() string {
	if opt.Network == "" {
		return "tcp4"
	}
	return opt.Network
}

func (opt *Options) getDialer() func() (Conn, error) {
	if opt.Dialer == nil {
		panic("Options Caller must use your own Dialer")
	}
	return opt.Dialer
}

func (opt *Options) getPoolSize() int {
	if opt.PoolSize == 0 {
		return 10
	}
	return opt.PoolSize
}

func (opt *Options) getDialTimeout() time.Duration {
	if opt.DialTimeout == 0 {
		return 5 * time.Second
	}
	return opt.DialTimeout
}

func (opt *Options) getPoolTimeout() time.Duration {
	if opt.PoolTimeout == 0 {
		return 1 * time.Second
	}
	return opt.PoolTimeout
}

func (opt *Options) getIdleTimeout() time.Duration {
	return opt.IdleTimeout
}

type connList struct {
	cns  []Conn
	mx   sync.Mutex
	len  int32 // atomic
	size int32
}

func newconnList(size int) *connList {
	return &connList{
		cns:  make([]Conn, 0, size),
		size: int32(size),
	}
}

func (l *connList) Len() int {
	return int(atomic.LoadInt32(&l.len))
}

// Reserve reserves place in the list and returns true on success. The
// caller must add or remove connection if place was reserved.
func (l *connList) Reserve() bool {
	len := atomic.AddInt32(&l.len, 1)
	reserved := len <= l.size
	if !reserved {
		atomic.AddInt32(&l.len, -1)
	}
	return reserved
}

// Add adds connection to the list. The caller must reserve place first.
func (l *connList) Add(cn Conn) {
	l.mx.Lock()
	l.cns = append(l.cns, cn)
	l.mx.Unlock()
}

// Remove closes connection and removes it from the list.
func (l *connList) Remove(cn Conn) error {
	defer l.mx.Unlock()
	l.mx.Lock()

	if cn == nil {
		atomic.AddInt32(&l.len, -1)
		return nil
	}
	// ensure Conn in l.cns
	for i, c := range l.cns {
		if c == cn {
			l.cns = append(l.cns[:i], l.cns[i+1:]...)
			atomic.AddInt32(&l.len, -1)
			return cn.Close()
		}
	}

	if l.closed() {
		return nil
	}
	panic("conn not found in the list")
}

func (l *connList) Replace(cn, newcn Conn) error {
	defer l.mx.Unlock()
	l.mx.Lock()

	for i, c := range l.cns {
		if c == cn {
			l.cns[i] = newcn
			return cn.Close()
		}
	}

	if l.closed() {
		return newcn.Close()
	}
	panic("conn not found in the list")
}

func (l *connList) Close() (retErr error) {
	l.mx.Lock()
	for _, c := range l.cns {
		if err := c.Close(); err != nil {
			retErr = err
		}
	}
	l.cns = nil
	atomic.StoreInt32(&l.len, 0)
	l.mx.Unlock()
	return retErr
}

func (l *connList) closed() bool {
	return l.cns == nil
}

type ConnPool struct {
	dialer func() (Conn, error)

	rl        *ratelimit.RateLimiter
	opt       *Options
	conns     *connList
	freeConns chan Conn

	_closed int32

	lastDialErr error
}

func NewConnPool(opt *Options) *ConnPool {
	p := &ConnPool{
		dialer:    opt.getDialer(),
		rl:        ratelimit.New(2*opt.getPoolSize(), time.Second),
		opt:       opt,
		conns:     newconnList(opt.getPoolSize()),
		freeConns: make(chan Conn, opt.getPoolSize()),
	}
	if p.opt.getIdleTimeout() > 0 {
		go p.reaper()
	}
	return p
}

func (p ConnPool) closed() bool {
	return atomic.LoadInt32(&p._closed) == 1
}

func (p ConnPool) isIdle(cn Conn) bool {
	return p.opt.getIdleTimeout() > 0 && time.Since(cn.LastUsed()) > p.opt.getIdleTimeout()
}

// First returns first non-idle connection from the pool or nil if
// there are no connections.
func (p ConnPool) First() Conn {
	for {
		select {
		case cn := <-p.freeConns:
			if p.isIdle(cn) {
				p.conns.Remove(cn)
				continue
			}
			return cn
		default:
			return nil
		}
	}
	panic("not reached")
}

// wait waits for free non-idle connection. It returns nil on timeout.
func (p ConnPool) wait() Conn {
	deadline := time.After(p.opt.getPoolTimeout())
	for {
		select {
		case cn := <-p.freeConns:
			if p.isIdle(cn) {
				p.Remove(cn)
				continue
			}
			return cn
		case <-deadline:
			return nil
		}
	}
	panic("not reached")
}

// Establish a new connection
func (p ConnPool) new() (Conn, error) {
	if p.rl.Limit() {
		err := fmt.Errorf(
			"redis: you open connections too fast (last error: %v)",
			p.lastDialErr,
		)
		return nil, err
	}

	cn, err := p.dialer()
	if err != nil {
		p.lastDialErr = err
		return nil, err
	}
	return cn, nil
}

// Get returns existed connection from the pool or creates a new one.
func (p ConnPool) Get() (Conn, error) {
	if p.closed() {
		return nil, errClosed
	}

	// Fetch first non-idle connection, if available.
	if cn := p.First(); cn != nil {
		return cn, nil
	}

	// Try to create a new one.
	if p.conns.Reserve() {
		cn, err := p.new()
		if err != nil {
			p.conns.Remove(nil)
			return nil, err
		}
		p.conns.Add(cn)
		return cn, nil
	}

	// Otherwise, wait for the available connection.
	if cn := p.wait(); cn != nil {
		return cn, nil
	}

	return nil, errPoolTimeout
}

func (p ConnPool) Put(cn Conn) error {
	if cn.Discard() != nil {
		return p.Remove(cn)
	}
	if p.opt.getIdleTimeout() > 0 {
		cn.SetLastUsed(time.Now())
	}
	p.freeConns <- cn
	return nil
}

func (p ConnPool) Remove(cn Conn) error {
	// Replace existing connection with new one and unblock waiter.
	newcn, err := p.new()
	if err != nil {
		return p.conns.Remove(cn)
	}
	err = p.conns.Replace(cn, newcn)
	p.freeConns <- newcn
	return err
}

// Len returns total number of connections.
func (p ConnPool) Len() int {
	return p.conns.Len()
}

// FreeLen returns number of free connections.
func (p ConnPool) FreeLen() int {
	return len(p.freeConns)
}

func (p ConnPool) Close() (retErr error) {
	if !atomic.CompareAndSwapInt32(&p._closed, 0, 1) {
		return errClosed
	}
	// Wait for app to free connections, but don't close them immediately.
	for i := 0; i < p.Len(); i++ {
		if cn := p.wait(); cn == nil {
			break
		}
	}
	// Close all connections.
	if err := p.conns.Close(); err != nil {
		retErr = err
	}
	return retErr
}

func (p ConnPool) reaper() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for _ = range ticker.C {
		if p.closed() {
			break
		}

		// pool.First removes idle connections from the pool and
		// returns first non-idle connection. So just put returned
		// connection back.
		if cn := p.First(); cn != nil {
			p.Put(cn)
		}
	}
}
