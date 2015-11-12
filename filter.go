package archer

import (
	"errors"
	"github.com/dongzerun/archer/hack"
	"github.com/dongzerun/archer/util"
)

var (
	InspectArgWrong = errors.New("Inspect only receive *ArrayResp")

	BadCommandError      = errors.New("error bad command")
	WrongArgumentCount   = errors.New("wrong argument count")
	WrongCommandKey      = errors.New("wrong command key")
	CommandForbidden     = errors.New("command forbidden")
	CommandNotSupported  = errors.New("command not supported")
	UnknowProxyOpType    = errors.New("Unknow args type for proxy command")
	BlackTimeUnavaliable = errors.New("black time unavaliable")
)

type Filter interface {
	Inspect(Resp) (string, error)
}

type StrFilter struct {
}

func (s *StrFilter) Inspect(r Resp) (string, error) {
	ar, ok := r.(*ArrayResp)
	if !ok {
		return "", InspectArgWrong
	}

	cmd := hack.String(util.UpperSlice(ar.Args[0].Args[0]))

	l := ar.Length() + 1

	// 黑名单
	if _, ok := blackList[cmd]; ok {
		return "", CommandForbidden
	}

	// 规则检查，参数数量
	rule, exists := reqrules[cmd]
	if !exists {
		return "", BadCommandError
	}

	for i, count := 0, len(rule); i < count; i++ {
		switch i {
		case RI_MinCount:
			if val := rule[i].(int); val != -1 && l < val {
				return "", WrongArgumentCount
			}
		case RI_MaxCount:
			if val := rule[i].(int); val != -1 && l > val {
				return "", WrongArgumentCount
			}
		}
	}

	return cmd, nil
}

type TrieFilter struct {
}

func (t *TrieFilter) Inspect(r Resp) error {
	panic("currently Trie Not Implemented")
	return nil
}
