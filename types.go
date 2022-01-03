package jackd

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type Client struct {
	conn    net.Conn
	buffer  *bufio.ReadWriter
	scanner *bufio.Scanner
	mutex   *sync.Mutex
}

type PutOpts struct {
	Priority uint32
	Delay    time.Duration
	TTR      time.Duration
}

func DefaultPutOpts() PutOpts {
	return PutOpts{
		Priority: 0,
		Delay:    0,
		TTR:      60 * time.Second,
	}
}

type ReleaseOpts struct {
	Priority uint32
	Delay    time.Duration
}

func DefaultReleaseOpts() ReleaseOpts {
	return ReleaseOpts{
		Priority: 0,
		Delay:    0,
	}
}
