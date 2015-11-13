package archer

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dongzerun/archer/hack"
	"github.com/dongzerun/archer/util"
	log "github.com/ngaut/logging"
)

type SessMana struct {
	l    sync.Mutex
	pool map[string]*Session

	idle time.Duration
}

func newSessMana(t time.Duration) *SessMana {
	sm := &SessMana{
		pool: make(map[string]*Session, 4096),
		idle: t,
	}
	go sm.CheckLoop()
	return sm
}

func (sm *SessMana) Put(remote string, s *Session) {
	log.Info("New Session Put to Session Management ", remote)
	sm.l.Lock()
	defer sm.l.Unlock()
	sm.pool[remote] = s
}

func (sm *SessMana) Del(remote string, s *Session) {
	log.Info("New Session Del from Session Management ", remote)
	sm.l.Lock()
	defer sm.l.Unlock()
	delete(sm.pool, remote)
}

func (sm *SessMana) CheckLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for id, s := range sm.pool {
				if sm.idle > 0 && time.Since(s.lastUsed) > sm.idle {
					sm.l.Lock()
					delete(sm.pool, id)
					sm.l.Unlock()
					log.Infof("client %s idle timeout quit", id)
					s.Close()
				}
			}
		}
	}
}

// for pipeline wrap Req and Resp with Sequence
type wrappedResp struct {
	seq  int64
	resp Resp
}

type Session struct {
	p *Proxy
	c net.Conn // client side Network Connection
	w *bufio.Writer
	r *bufio.Reader

	resps chan *wrappedResp
	cmds  chan *wrappedResp
	//out-of-order store temporary
	ooo map[int64]Resp

	conCurrency chan int

	quitChan chan int
	closed   bool
	wg       util.WaitGroupWrapper

	// pipeline used seq
	reqSequence  int64
	respSequence int64

	lastUsed time.Time
	remote   string
}

func NewSession(p *Proxy, c net.Conn) *Session {
	s := &Session{
		p: p,
		c: c,
		w: bufio.NewWriter(c),
		r: bufio.NewReader(c),
		//pipeline length 4096
		cmds:  make(chan *wrappedResp, p.pc.pipeLength),
		resps: make(chan *wrappedResp, p.pc.pipeLength),
		//store temporary Resp for Max p.pc.conCurrency
		//out-of-order store temporary
		ooo: make(map[int64]Resp, p.pc.conCurrency),
		//max dispatch concurrency goroutine per session
		conCurrency: make(chan int, p.pc.conCurrency),
		quitChan:    make(chan int, 1),
		lastUsed:    time.Now(),
		remote:      c.RemoteAddr().String(),
	}

	for i := 0; i < p.pc.conCurrency; i++ {
		s.conCurrency <- 1
	}

	return s
}

func (s *Session) ReadLoop() {
	for !s.closed {

		cmd, err := ReadProtocol(s.r)
		if err != nil && err != io.EOF {
			log.Warningf("%s ReadLoop read err: %s", s.c.RemoteAddr().String(), err)
			continue
		}
		if err == io.EOF {
			log.Infof("%s ReadLoop read EOF just quit ", s.c.RemoteAddr().String())
			s.Close()
			goto quit
		}

		log.Info("ReadLoop receive cmd ", cmd.Type(), cmd.String(), s.reqSequence)
		s.cmds <- WrappedResp(cmd, s.reqSequence)

		s.lastUsed = time.Now()
		atomic.AddInt64(&s.reqSequence, 1)
	}
quit:
	log.Warning("quit ReadLoop")
}

func (s *Session) Dispatch() {
	for {
		select {
		case c := <-s.cmds:
			command, err := s.p.filter.Inspect(c.resp)
			if err != nil {
				s.resps <- WrappedErrorResp([]byte(err.Error()), c.seq)
				continue
			}

			log.Info("Dispatch after Inspect cmd: ", command)
			ar := c.resp.(*ArrayResp)
			switch command {
			case "PING":
				s.resps <- WrappedPONGResp(c.seq)
				continue
			case "QUIT":
				s.resps <- WrappedOKResp(c.seq)
				s.Close()
				goto quit
			case "SELECT":
				s.resps <- WrappedOKResp(c.seq)
				continue
			case "INFO":
				//TODO: implement INFO command
				s.resps <- WrappedOKResp(c.seq)
			case "MSET":
				s.Route(ar, c.seq, "MSET")
			case "MGET":
				s.Route(ar, c.seq, "MGET")
			case "DEL":
				s.Route(ar, c.seq, "DEL")
			default:
				s.Route(ar, c.seq, "")
			}

		case <-s.quitChan:
			goto quit
		}
	}
quit:
	log.Warning("quit Dispatch")
}

func (s *Session) Route(req *ArrayResp, seq int64, multop string) {
	//channel timeout ???
	<-s.conCurrency

	switch multop {
	case "MSET":
		go s.MSET(req, seq)
	case "MGET":
		go s.MGET(req, seq)
	case "DEL":
		go s.DEL(req, seq)
	default:
		go s.DefaultOP(req, seq)
	}
}

func (s *Session) DefaultOP(req *ArrayResp, seq int64) {
	defer func() {
		s.conCurrency <- 1
	}()
	resp, err := s.ExecWithRedirect(req, true)
	if err != nil {
		errinfo := fmt.Errorf("proxy internal error %s", err.Error())
		s.resps <- WrappedErrorResp([]byte(errinfo.Error()), seq)
		return
	}
	s.resps <- WrappedResp(resp, seq)
}

func (s *Session) WriteLoop() {
	for {
		select {
		case r := <-s.resps:
			log.Info("WriteLoop Read Response ", r.resp.String(), r.seq)
			// req and resp sequence must equal, thus we can ensure pipeline seq
			resp := r.resp
			if r.seq > s.respSequence {
				log.Warningf("WriteLoop receive resp unorder %d expected %d", r.seq, s.respSequence)
				s.ooo[r.seq] = r.resp
				// we will find r.respSequence in s.ooo
				var ok bool
				resp, ok = s.ooo[s.respSequence]
				if !ok {
					// can't find, and wait again
					// TODO:: we can't wait forever, may timeout after 1 second or p.pc.conCurrency-1 ???
					// currently just p.pc.conCurrency-1 seq
					if r.seq-s.respSequence < int64(s.p.pc.conCurrency-1) {
						continue
					}
					log.Warningf("WriteLoop receive resp unorder for %d, we send ERROR instead", s.p.pc.conCurrency-1)
					er := &ErrorResp{}
					er.Rtype = ErrorType
					er.Args = append(er.Args, []byte("proxy internal error pipeline unorder"))
					resp = er
				}
			}

			// we already discard r.seq response
			if r.seq < s.respSequence {
				log.Warningf("WriteLoop receive %d < %d just discard resp:%s", r.seq, s.respSequence, r.resp.String())
				continue
			}
			atomic.AddInt64(&s.respSequence, 1)

			err := WriteProtocol(s.w, resp)
			if err != nil {
				log.Warning("WriteLoop WriteProtocol err ", err.Error())
			}
		case <-s.quitChan:
			goto quit
		}
	}
quit:
	log.Warning("quit WriteLoop")
}

func WrappedErrorResp(reason []byte, seq int64) *wrappedResp {
	er := &ErrorResp{}
	er.Rtype = ErrorType
	er.Args = append(er.Args, reason)
	return &wrappedResp{
		resp: er,
		seq:  seq,
	}
}

func WrappedOKResp(seq int64) *wrappedResp {
	sr := &SimpleResp{}
	sr.Args = append(sr.Args, OK)
	sr.Rtype = SimpleType
	return &wrappedResp{
		resp: sr,
		seq:  seq,
	}
}

func WrappedPONGResp(seq int64) *wrappedResp {
	sr := &SimpleResp{}
	sr.Args = append(sr.Args, PONG)
	sr.Rtype = SimpleType
	return &wrappedResp{
		resp: sr,
		seq:  seq,
	}
}

// caller call 	defer s.p.cluster.PutConn(conn)
func WrappedResp(r Resp, seq int64) *wrappedResp {
	return &wrappedResp{
		resp: r,
		seq:  seq,
	}
}

// caller call 	defer s.p.cluster.PutConn(conn)
func (s *Session) GetRedisConnByKey(key []byte, slave bool) (*RedisConn, error) {
	//ensure req.Args[0].Args[1] is key
	conn, err := s.p.cluster.GetConn(key, slave)
	if err != nil {
		return nil, err
	}

	rc, ok := conn.(*RedisConn)
	if !ok {
		return nil, fmt.Errorf("proxy error: GetRedisConnByKey failed")
	}

	return rc, nil
}

func (s *Session) GetRedisConnByID(id string) (*RedisConn, error) {
	conn, err := s.p.cluster.pools[id].Get()
	if err != nil {
		return nil, err
	}

	rc, ok := conn.(*RedisConn)
	if !ok {
		return nil, fmt.Errorf("proxy error: GetRedisConnByID failed")
	}

	return rc, nil
}

//tp type: moved ask normal
func (s *Session) Redirect(tp string, req *ArrayResp, target string) Resp {
	//reclaim RedisConn
	rc, err := s.GetRedisConnByID(target)
	if err != nil {
		log.Warning("Session forward get conn not RedisConn")
		er := &ErrorResp{}
		er.Rtype = ErrorType
		er.Args = append(er.Args, []byte("proxy internal error pool conn not RedisConn"))
		return er
	}
	defer s.p.cluster.PutConn(rc)

	switch tp {
	case "ASK":
		//ask first sending a ASKING command
		ar := &ArrayResp{}
		ar.Rtype = ArrayType
		br := &BulkResp{}
		br.Rtype = BulkType
		br.Args = [][]byte{[]byte("ASKING")}
		ar.Args = append(ar.Args, br)
		err = WriteProtocol(rc.w, ar)
	}

	var resp Resp
	resp, err = s.ExecOnce(rc, req)
	if err == nil {
		return resp
	}

	er := &ErrorResp{}
	er.Rtype = ErrorType
	er.Args = append(er.Args, []byte(err.Error()))
	return er
}

func (s *Session) ExecWithRedirect(req *ArrayResp, redirect bool) (Resp, error) {
	//ensure req.Args[1].Args[0] is key
	rc, err := s.GetRedisConnByKey(req.Args[1].Args[0], false)
	if err != nil {
		log.Warning("ExecWithRedirect GetRedisConnByKey get conn failed ", err)
		return nil, err
	}
	//reclaim RedisConn
	defer s.p.cluster.PutConn(rc)

	var resp Resp
	resp, err = s.ExecOnce(rc, req)
	if err != nil {
		log.Warning("Session forward ReadProtocol error ", err)
		return nil, err
	}

	er, ok := resp.(*ErrorResp)
	if ok {
		//-MOVED 15495 10.10.200.11:6481 redirect to target
		//-ASK 15495 10.10.200.11:6481 redirect to target,send ASKING command and then real ArrayResp
		//handle error response
		e := strings.Fields(hack.String(er.Args[0]))
		if len(e) == 3 && redirect {
			switch e[0] {
			case "MOVED":
				//we need reload Slots Info
				s.p.cluster.topo.reloadChan <- 1
				resp = s.Redirect("MOVED", req, e[2])
			case "ASK":
				//need not reload Slots Info, wait Migrate Done
				resp = s.Redirect("ASK", req, e[2])
			}
		}
	}
	return resp, nil
}

func (s *Session) ExecOnce(c *RedisConn, req *ArrayResp) (Resp, error) {
	err := WriteProtocol(c.w, req)
	if err != nil {
		return nil, err
	}

	var resp Resp
	resp, err = ReadProtocol(c.r)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *Session) Close() {
	if s.closed {
		return
	}

	s.closed = true
	close(s.quitChan)
	s.p.sm.Del(s.remote, s)

	if s.c != nil {
		s.c.Close()
	}
}

func (s *Session) Serve() {
	// run one by one
	s.wg.Wrap(s.WriteLoop)
	s.wg.Wrap(s.Dispatch)
	s.wg.Wrap(s.ReadLoop)

	s.wg.Wait()
}
