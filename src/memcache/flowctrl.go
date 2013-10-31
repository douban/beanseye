package memcache

import (
	"errors"
	"fmt"
	"time"
)

type FlowServerConn struct {
	conn  *ServerConn
	stamp time.Time
}

func (c *FlowServerConn) Close() {
	c.conn.Close()
}

func NewFlowServerConn(c *ServerConn) *FlowServerConn {
	return &FlowServerConn{conn: c, stamp: time.Now()}
}

type FlowController struct {
	max_qps, curr_qps int
	conn_timeout      float32
	flow, pipe        chan *FlowServerConn
	timeout           time.Duration
}

func NewFlowController(maxqps int, timeout float32) *FlowController {
	f := new(FlowController)
	f.max_qps = maxqps
	f.curr_qps = 0
	f.conn_timeout = timeout
	f.flow = make(chan *FlowServerConn, 1+int(f.conn_timeout*float32(f.max_qps)))
	f.pipe = nil
	duration_string := fmt.Sprintf("%fs", timeout)
	f.timeout, _ = time.ParseDuration(duration_string)
	return f
}

func (f *FlowController) Put(c *ServerConn) (ok bool, err error) {
	flow_c := NewFlowServerConn(c)
	select {
	case f.flow <- flow_c:
	default:
		return false, errors.New("channel is full")
	}
	return true, nil
}

func (f *FlowController) CanTransmitNow() bool {
	return true
}

func (f *FlowController) WhenToTransmit() time.Duration {
	r, _ := time.ParseDuration("1s")
	return r
}

func (f *FlowController) transmit() {
	tran := func() {
		conn := <-f.flow // blocking mode is ok
		if time.Now().Sub(conn.stamp) > f.timeout {
			// timeout , Close and drop
			conn.Close()
			return
		}
		select {
		// do not block in sending procedure
		case f.pipe <- conn:
			return
		default:
			// ignore the pipe full time
			// because caller handle it.
			conn.Close()
			return
		}
	}
	for {
		if f.CanTransmitNow() {
			tran()
		} else {
			to_next := f.WhenToTransmit()
			time.AfterFunc(to_next, tran)
		}
	}
}

func (f *FlowController) Bind(output chan *FlowServerConn) bool {
	if f.pipe == nil {
		f.pipe = output
		go f.transmit()
		return true
	} else {
		return false
	}
}
