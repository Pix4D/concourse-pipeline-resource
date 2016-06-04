// This file was generated by counterfeiter
package fakes

import (
	"bufio"
	"net"
	"sync"

	"github.com/concourse/retryhttp"
)

type FakeDoHijackCloserFactory struct {
	NewDoHijackCloserStub        func(c net.Conn, r *bufio.Reader) retryhttp.DoHijackCloser
	newDoHijackCloserMutex       sync.RWMutex
	newDoHijackCloserArgsForCall []struct {
		c net.Conn
		r *bufio.Reader
	}
	newDoHijackCloserReturns struct {
		result1 retryhttp.DoHijackCloser
	}
}

func (fake *FakeDoHijackCloserFactory) NewDoHijackCloser(c net.Conn, r *bufio.Reader) retryhttp.DoHijackCloser {
	fake.newDoHijackCloserMutex.Lock()
	fake.newDoHijackCloserArgsForCall = append(fake.newDoHijackCloserArgsForCall, struct {
		c net.Conn
		r *bufio.Reader
	}{c, r})
	fake.newDoHijackCloserMutex.Unlock()
	if fake.NewDoHijackCloserStub != nil {
		return fake.NewDoHijackCloserStub(c, r)
	} else {
		return fake.newDoHijackCloserReturns.result1
	}
}

func (fake *FakeDoHijackCloserFactory) NewDoHijackCloserCallCount() int {
	fake.newDoHijackCloserMutex.RLock()
	defer fake.newDoHijackCloserMutex.RUnlock()
	return len(fake.newDoHijackCloserArgsForCall)
}

func (fake *FakeDoHijackCloserFactory) NewDoHijackCloserArgsForCall(i int) (net.Conn, *bufio.Reader) {
	fake.newDoHijackCloserMutex.RLock()
	defer fake.newDoHijackCloserMutex.RUnlock()
	return fake.newDoHijackCloserArgsForCall[i].c, fake.newDoHijackCloserArgsForCall[i].r
}

func (fake *FakeDoHijackCloserFactory) NewDoHijackCloserReturns(result1 retryhttp.DoHijackCloser) {
	fake.NewDoHijackCloserStub = nil
	fake.newDoHijackCloserReturns = struct {
		result1 retryhttp.DoHijackCloser
	}{result1}
}

var _ retryhttp.DoHijackCloserFactory = new(FakeDoHijackCloserFactory)