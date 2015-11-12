package archer

import (
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
		br.Args = [][]byte{[]byte("GET"), req.Args[i+1].Args[0]}
		ar.Args = append(ar.Args, br)
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
		br.Args = [][]byte{[]byte("SET"), req.Args[i+1].Args[0], req.Args[i+2].Args[0]}
		ar.Args = append(ar.Args, br)
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
		br.Args = [][]byte{[]byte("DEL"), req.Args[i+1].Args[0]}
		ar.Args = append(ar.Args, br)

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

		if ir.Args[0][0] == byte('1') {
			del += 1
		}
	}
	r := &IntResp{}
	r.Rtype = IntType
	r.Args = append(r.Args, []byte(string(del)))
	s.resps <- WrappedResp(r, seq)
	return
}
