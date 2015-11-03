package simpleproxy

import (
	"bufio"
	"bytes"
	"io"
	"net"

	log "github.com/ngaut/logging"
)

type Session struct {
	p     *Proxy
	c     net.Conn
	w     *bufio.Writer
	r     *bufio.Reader
	resps chan Resp
	cmds  chan *ArrayResp

	quitChan chan int
	closed   bool
	wg       WaitGroupWrapper
}

func NewSession(p *Proxy, c net.Conn) *Session {
	tcpc, ok := c.(*net.TCPConn)
	if ok {
		tcpc.SetNoDelay(true)
	}
	return &Session{
		p: p,
		c: tcpc,
		w: bufio.NewWriter(c),
		r: bufio.NewReader(c),
		//pipeline length 4096
		cmds:     make(chan *ArrayResp, 4096),
		resps:    make(chan Resp, 4096),
		quitChan: make(chan int, 1),
	}
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

		if c, ok := cmd.(*ArrayResp); ok {
			s.cmds <- c
		} else {
			log.Warning("%s ReadLoop read non-command: %s", s.c.RemoteAddr().String(), c.String())
		}
	}
quit:
	log.Warning("quit ReadLoop")
}

func (s *Session) Dispatch() {
	// simple node forward Proxy
	back := NewBackend(s.p.h, s.p.p)

	go func() {
		for !s.closed {
			r, e := ReadProtocol(back.r)
			if e != nil {
				// read tcp4 127.0.0.1:56930->127.0.0.1:6379: use of closed network connection
				// 2015/11/03 20:32:13 session.go:63: [warning] quit ReadLoop
				log.Warning("Dispatch ReadProtocl error: ", e)
				break
			}
			s.resps <- r
		}
	}()

	for {
		select {
		case c := <-s.cmds:
			if bytes.Equal(c.Args[0].Args[0], PING) {
				sr := &SimpleResp{}
				sr.Args = append(sr.Args, PONG)
				sr.Rtype = SimpleType
				s.resps <- sr
				continue
			}

			if bytes.Equal(c.Args[0].Args[0], QUIT) {
				sr := &SimpleResp{}
				sr.Args = append(sr.Args, OK)
				sr.Rtype = SimpleType
				s.resps <- sr
				s.Close()
				goto quit
			}

			_, err := back.w.Write(c.Encode())
			if err != nil {
				log.Warning("Dispatch write error: ", err)
			}
			err = back.w.Flush()
			if err != nil {
				log.Warning("Dispatch flush error: ", err)
			}

		case <-s.quitChan:
			goto quit
		}
	}
quit:
	log.Warning("quit Dispatch")
	back.c.Close()
}

func (s *Session) WriteLoop() {
	for {
		select {
		case r := <-s.resps:
			// log.Info("WriteLoop Read Response ", r.Encode())
			_, err := s.w.Write(r.Encode())
			if err != nil {
				// log.Info("writeloop write error ", err)
			}
			err = s.w.Flush()
			if err != nil {
				// log.Info("writeloop flush error ", err)
			}
		case <-s.quitChan:
			goto quit
		}
	}
quit:
	log.Warning("quit WriteLoop")
}

func (s *Session) Close() {
	if !s.closed {
		s.closed = true
		close(s.quitChan)
	}
}

func (s *Session) Serve() {
	// run one by one
	s.wg.Wrap(s.WriteLoop)
	s.wg.Wrap(s.Dispatch)
	s.wg.Wrap(s.ReadLoop)

	s.wg.Wait()
}
