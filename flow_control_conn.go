package shadowtls

import (
	"io"
	"net"
	"sync"
	"time"
)

const flowControlChunkSize = 16 * 1024

type flowControlledConn struct {
	net.Conn
	upstreamLimiter   *flowControlLimiter
	downstreamLimiter *flowControlLimiter
}

func newFlowControlledConn(conn net.Conn, upstreamBytesPerSecond int64, downstreamBytesPerSecond int64) net.Conn {
	if upstreamBytesPerSecond <= 0 && downstreamBytesPerSecond <= 0 {
		return conn
	}
	return &flowControlledConn{
		Conn:              conn,
		upstreamLimiter:   newFlowControlLimiter(upstreamBytesPerSecond),
		downstreamLimiter: newFlowControlLimiter(downstreamBytesPerSecond),
	}
}

func (c *flowControlledConn) Read(p []byte) (n int, err error) {
	n, err = c.Conn.Read(p)
	c.upstreamLimiter.WaitN(n)
	return
}

func (c *flowControlledConn) Write(p []byte) (n int, err error) {
	if c.downstreamLimiter == nil {
		return c.Conn.Write(p)
	}
	for len(p) > 0 {
		writeLen := len(p)
		if writeLen > flowControlChunkSize {
			writeLen = flowControlChunkSize
		}
		c.downstreamLimiter.WaitN(writeLen)
		wroteN, writeErr := c.Conn.Write(p[:writeLen])
		n += wroteN
		if writeErr != nil {
			return n, writeErr
		}
		if wroteN != writeLen {
			return n, io.ErrShortWrite
		}
		p = p[wroteN:]
	}
	return
}

func (c *flowControlledConn) Upstream() any {
	return c.Conn
}

type flowControlLimiter struct {
	bytesPerSecond int64
	access         sync.Mutex
	nextTime       time.Time
}

func newFlowControlLimiter(bytesPerSecond int64) *flowControlLimiter {
	if bytesPerSecond <= 0 {
		return nil
	}
	return &flowControlLimiter{
		bytesPerSecond: bytesPerSecond,
	}
}

func (l *flowControlLimiter) WaitN(n int) {
	if l == nil || n <= 0 {
		return
	}
	for n > 0 {
		waitLen := n
		if waitLen > flowControlChunkSize {
			waitLen = flowControlChunkSize
		}
		l.wait(waitLen)
		n -= waitLen
	}
}

func (l *flowControlLimiter) wait(n int) {
	l.access.Lock()
	now := time.Now()
	if l.nextTime.Before(now) {
		l.nextTime = now
	}
	waitDuration := l.nextTime.Sub(now)
	advanceDuration := time.Duration((int64(n)*int64(time.Second) + l.bytesPerSecond - 1) / l.bytesPerSecond)
	l.nextTime = l.nextTime.Add(advanceDuration)
	l.access.Unlock()
	if waitDuration > 0 {
		time.Sleep(waitDuration)
	}
}
