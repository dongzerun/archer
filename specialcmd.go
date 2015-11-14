package archer

import (
	"bytes"

	"github.com/dongzerun/archer/util"
	log "github.com/ngaut/logging"
)

func (s *Session) MGET(req *ArrayResp, seq int64) {
	defer func() {
		s.conCurrency <- 1
	}()
	var failed bool
	mget := &ArrayResp{}
	mget.Rtype = ArrayType
	mget.Args = make([]*BulkResp, req.Length())
	for i := 0; i < req.Length(); i++ {
		ar := &ArrayResp{}
		ar.Rtype = ArrayType
		br := &BulkResp{}
		br.Rtype = BulkType
		br.Args = [][]byte{[]byte("GET")}
		ar.Args = append(ar.Args, br)

		br1 := &BulkResp{}
		br1.Rtype = BulkType
		br1.Args = [][]byte{req.Args[i+1].Args[0]}
		ar.Args = append(ar.Args, br1)

		resp, err := s.ExecWithRedirect(ar, true)
		if err != nil {
			log.Warning("Session MGET ExecWithRedirect wrong ", ar.String())
			failed = true
			break
		}
		br, ok := resp.(*BulkResp)
		if !ok {
			log.Warning("Session MGET ExecWithRedirect  wrong must get BulkResp")
			failed = true
			break
		}
		mget.Args[i] = br
	}

	if failed {
		s.resps <- WrappedErrorResp([]byte("proxy internal MGET failed"), seq)
		return
	}
	s.resps <- WrappedResp(mget, seq)
}

func (s *Session) MSET(req *ArrayResp, seq int64) {
	defer func() {
		s.conCurrency <- 1
	}()

	if req.Length()%2 != 0 {
		s.resps <- WrappedErrorResp([]byte("MSET args count must Even"), seq)
		return
	}

	var failed bool
	for i := 0; i < req.Length(); i += 2 {
		ar := &ArrayResp{}
		ar.Rtype = ArrayType
		br := &BulkResp{}
		br.Rtype = BulkType
		br.Args = [][]byte{[]byte("SET")}
		ar.Args = append(ar.Args, br)

		br1 := &BulkResp{}
		br1.Rtype = BulkType
		br1.Args = [][]byte{req.Args[i+1].Args[0]}
		ar.Args = append(ar.Args, br1)

		br2 := &BulkResp{}
		br2.Rtype = BulkType
		br2.Args = [][]byte{req.Args[i+2].Args[0]}
		ar.Args = append(ar.Args, br2)

		resp, err := s.ExecWithRedirect(ar, true)
		if err != nil {
			log.Warning("Session MSET ExecWithRedirect wrong ", ar.String())
			failed = true
			break
		}
		_, ok := resp.(*SimpleResp)
		if !ok {
			log.Warning("Session MSET ExecWithRedirect  wrong must get SimpleResp ")
			failed = true
			break
		}
	}

	if failed {
		s.resps <- WrappedErrorResp([]byte("MGET partitial failed"), seq)
		return
	}
	s.resps <- WrappedOKResp(seq)
}

func (s *Session) DEL(req *ArrayResp, seq int64) {
	defer func() {
		s.conCurrency <- 1
	}()

	var del int
	for i := 0; i < req.Length(); i++ {
		ar := &ArrayResp{}
		ar.Rtype = ArrayType
		br := &BulkResp{}
		br.Rtype = BulkType
		br.Args = [][]byte{[]byte("DEL")}
		ar.Args = append(ar.Args, br)

		br1 := &BulkResp{}
		br1.Rtype = BulkType
		br1.Args = [][]byte{req.Args[i+1].Args[0]}
		ar.Args = append(ar.Args, br1)

		resp, err := s.ExecWithRedirect(ar, true)
		if err != nil {
			log.Warning("Session DEL ExecWithRedirect wrong ", ar.String())
			continue
		}
		ir, ok := resp.(*IntResp)
		if !ok {
			log.Warning("Session DEL ExecWithRedirect  wrong must get IntResp")
			continue
		}
		log.Info("DEL resp ", ir.Rtype, string(ir.Args[0]))
		if bytes.Equal(ir.Args[0], []byte("1")) {
			del += 1
		}
	}
	r := &IntResp{}
	r.Rtype = IntType
	r.Args = append(r.Args, util.Iu32tob(del))
	s.resps <- WrappedResp(r, seq)
	return
}
