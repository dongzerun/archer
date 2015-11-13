package archer

const (
	RI_MinCount = iota
	RI_MaxCount
)

var reqrules = map[string][]interface{}{
	// proxy special command
	"PROXY":  []interface{}{2, 5},
	"SELECT": []interface{}{2, 2},
	"PING":   []interface{}{1, 1},
	"QUIT":   []interface{}{1, 1},
	// key
	"DEL":       []interface{}{2, 2001},
	"TYPE":      []interface{}{2, 2},
	"EXISTS":    []interface{}{2, 2},
	"EXPIRE":    []interface{}{3, 3},
	"EXPIREAT":  []interface{}{3, 3},
	"TTL":       []interface{}{2, 2},
	"PTTL":      []interface{}{2, 2},
	"PERSIST":   []interface{}{2, 2},
	"PEXPIRE":   []interface{}{3, 3},
	"PEXPIREAT": []interface{}{3, 3},
	"RENAME":    []interface{}{3, 3},
	"RENAMENX":  []interface{}{3, 3},
	"DUMP":      []interface{}{2, 2},
	"RESTORE":   []interface{}{4, 4},
	// bit

	"SETBIT":   []interface{}{4, 4},
	"BITCOUNT": []interface{}{2, 2},
	"GETBIT":   []interface{}{3, 3},

	// string
	"GET":         []interface{}{2, 2},
	"MGET":        []interface{}{2, 2001},
	"GETRANGE":    []interface{}{4, 4},
	"GETSET":      []interface{}{3, 3},
	"SET":         []interface{}{3, 6},
	"MSET":        []interface{}{3, 4001},
	"SETEX":       []interface{}{4, 4},
	"SETNX":       []interface{}{3, 3},
	"PSETEX":      []interface{}{4, 4},
	"SETRANGE":    []interface{}{4, 4},
	"STRLEN":      []interface{}{2, 2},
	"INCR":        []interface{}{2, 2},
	"DECR":        []interface{}{2, 2},
	"INCRBY":      []interface{}{3, 3},
	"DECRBY":      []interface{}{3, 3},
	"INCRBYFLOAT": []interface{}{3, 3},
	"APPEND":      []interface{}{3, 3},
	// hash
	"HGET":         []interface{}{3, 3},
	"HSET":         []interface{}{4, 4},
	"HMGET":        []interface{}{3, -1},
	"HMSET":        []interface{}{4, -1},
	"HGETALL":      []interface{}{2, 2},
	"HLEN":         []interface{}{2, 2},
	"HDEL":         []interface{}{3, -1},
	"HEXISTS":      []interface{}{3, 3},
	"HINCRBY":      []interface{}{4, 4},
	"HINCRBYFLOAT": []interface{}{4, 4},
	"HKEYS":        []interface{}{2, 2},
	"HSETNX":       []interface{}{4, 4},
	"HVALS":        []interface{}{2, 2},
	// set
	"SADD":        []interface{}{3, -1},
	"SCARD":       []interface{}{2, 2},
	"SISMEMBER":   []interface{}{3, 3},
	"SMEMBERS":    []interface{}{2, 2},
	"SREM":        []interface{}{3, -1},
	"SPOP":        []interface{}{2, 2},
	"SRANDMEMBER": []interface{}{2, 3},
	// "SMOVE":       []interface{}{4, 4},
	// list
	"LPUSH":   []interface{}{3, -1},
	"RPUSH":   []interface{}{3, -1},
	"LPOP":    []interface{}{2, 2},
	"RPOP":    []interface{}{2, 2},
	"LINDEX":  []interface{}{3, 3},
	"LINSERT": []interface{}{5, 5},
	"LTRIM":   []interface{}{4, 4},
	"LRANGE":  []interface{}{4, 4},
	"LLEN":    []interface{}{2, 2},
	"LPUSHX":  []interface{}{3, 3},
	"RPUSHX":  []interface{}{3, 3},
	"LSET":    []interface{}{4, 4},
	"LREM":    []interface{}{4, 4},
	// zset
	"ZADD":             []interface{}{4, -1},
	"ZCARD":            []interface{}{2, 2},
	"ZCOUNT":           []interface{}{4, 4},
	"ZRANK":            []interface{}{3, 3},
	"ZREVRANK":         []interface{}{3, 3},
	"ZRANGE":           []interface{}{4, 5},
	"ZREVRANGE":        []interface{}{4, 5},
	"ZRANGEBYSCORE":    []interface{}{4, -1},
	"ZREVRANGEBYSCORE": []interface{}{4, -1},
	"ZREM":             []interface{}{3, -1},
	"ZREMRANGEBYRANK":  []interface{}{4, 4},
	"ZREMRANGEBYSCORE": []interface{}{4, 4},
	"ZINCRBY":          []interface{}{4, 4},
	"ZSCORE":           []interface{}{3, 3},
	"ZRANGEBYLEX":      []interface{}{4, 7},
	"ZLEXCOUNT":        []interface{}{4, 4},
	"ZREMRANGEBYLEX":   []interface{}{4, 4},
	//finite zset
	"XADD":        []interface{}{4, -1},
	"XINCRBY":     []interface{}{4, 9},
	"XRANGE":      []interface{}{4, 5},
	"XREVRANGE":   []interface{}{4, 5},
	"XSCORE":      []interface{}{3, 3},
	"XREM":        []interface{}{3, -1},
	"XCARD":       []interface{}{2, 2},
	"XSETOPTIONS": []interface{}{3, 7},
	"XGETFINITY":  []interface{}{2, 2},
	"XGETPRUNING": []interface{}{2, 2},
}

var specList = map[string]bool{
	"INFO": true,
	// "RENAME":   true,
	// "RENAMENX": true,
	"MGET": true,
	"MSET": true,
	"DEL":  true,
	// "MSETNX":      true,
	// "RPOPLPUSH":   true,
	// "SDIFF":       true,
	// "SDIFFSTORE":  true,
	// "SINTER":      true,
	// "SINTERSTORE": true,
	// "SMOVE":       true,
	// "ZUNIONSTORE": true,
	// "ZINTERSTORE": true,
}

var blackList = map[string]bool{
	"BGREWRITEAOF": true,
	"BGSAVE":       true,
	"BITOP":        true,
	"BLPOP":        true,
	"BRPOP":        true,
	"BRPOPLPUSH":   true,
	"CLIENT":       true,
	"CONFIG":       true,
	"DBSIZE":       true,
	"DEBUG":        true,
	"DISCARD":      true,
	"EXEC":         true,
	"FLUSHALL":     true,
	"FLUSHDB":      true,
	"KEYS":         true,
	"LASTSAVE":     true,
	"MONITOR":      true,
	"MOVE":         true,
	"MSETNX":       true,
	"MULTI":        true,
	"OBJECT":       true,
	"PSUBSCRIBE":   true,
	"PUBLISH":      true,
	"PUNSUBSCRIBE": true,
	"RANDOMKEY":    true,
	"RENAME":       true,
	"RENAMENX":     true,
	"SAVE":         true,
	"SCAN":         true,
	"SSCAN":        true,
	"HSCAN":        true,
	"ZSCAN":        true,
	"SCRIPT":       true,
	"SHUTDOWN":     true,
	"SLAVEOF":      true,
	"SLOWLOG":      true,
	"SORT":         true,
	"SUBSCRIBE":    true,
	"SYNC":         true,
	"SDIFF":        true,
	"SDIFFSTORE":   true,
	"SINTER":       true,
	"SINTERSTORE":  true,
	"SMOVE":        true,
	"SUNION":       true,
	"SUNIONSTORE":  true,
	"TIME":         true,
	"UNSUBSCRIBE":  true,
	"UNWATCH":      true,
	"WATCH":        true,
	"ZUNIONSTORE":  true,
	"ZINTERSTORE":  true,
}
