package gccp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type Pool struct {
	address     string
	connections chan *grpc.ClientConn
	options     []grpc.DialOption
	size        int
	mu          sync.Mutex
	timeout     time.Duration
}

func NewPool(address string, size int, timeout time.Duration, options ...grpc.DialOption) (*Pool, error) {
	pool := &Pool{
		connections: make(chan *grpc.ClientConn, size),
		size:        size,
		address:     address,
		options:     options,
		mu:          sync.Mutex{},
		timeout:     timeout,
	}

	for range size {
		conn, err := grpc.NewClient(address, options...)
		if err != nil {
			return nil, fmt.Errorf("failed to dial: %w", err)
		}
		pool.connections <- conn
	}

	return pool, nil
}

func (p *Pool) Get() (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var conn *grpc.ClientConn

loop:
	for {
		select {
		case conn = <-p.connections:
			switch conn.GetState() {
			case connectivity.Ready, connectivity.Idle:
				break loop
			case connectivity.Connecting, connectivity.TransientFailure:
				if p.waitForConnectionReady(conn) {
					break loop
				}
			case connectivity.Shutdown:
			}

			_ = conn.Close()
			conn = nil
		default:
			// Pool is empty, create a new connection
			break loop
		}
	}

	if conn == nil {
		var err error

		conn, err = grpc.NewClient(p.address, p.options...)
		if err != nil {
			return nil, fmt.Errorf("failed to create new client: %w", err)
		}
	}

	return conn, nil
}

func (p *Pool) waitForConnectionReady(conn *grpc.ClientConn) bool {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel() // Ensure resources are cleaned up

	if conn.WaitForStateChange(ctx, conn.GetState()) {
		return conn.GetState() == connectivity.Ready
	}

	return false
}

func (p *Pool) Release(conn *grpc.ClientConn) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if conn.GetState() == connectivity.Shutdown || len(p.connections) >= p.size {
		// Don't put a closed connection back into the pool
		// or pool is full
		_ = conn.Close() // Handle close error if necessary

		return
	}

	p.connections <- conn
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		select {
		case conn := <-p.connections:
			_ = conn.Close()
		default:
			// Pool is empty
			return
		}
	}
}
