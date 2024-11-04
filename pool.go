package gccp

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// Pool represents a pool of gRPC client connections.
type Pool struct {
	address     string
	connections chan *grpc.ClientConn
	options     []grpc.DialOption
	size        int
	mu          sync.Mutex
}

// NewPool creates a new pool of gRPC client connections.
// It initializes the pool with the given address, size, timeout, and options.
func NewPool(address string, size int, timeout time.Duration, options ...grpc.DialOption) (*Pool, error) {
	pool := &Pool{
		connections: make(chan *grpc.ClientConn, size),
		size:        size,
		address:     address,
		options:     options,
		mu:          sync.Mutex{},
	}

	// Initialize the pool with connections
	for range size {
		conn, err := grpc.NewClient(address, options...)
		if err != nil {
			return nil, fmt.Errorf("failed to dial: %w", err)
		}
		pool.connections <- conn
	}

	return pool, nil
}

// Get retrieves a connection from the pool.
// If the pool is empty, it creates a new connection.
func (p *Pool) Get() (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

loop:
	for {
		select {
		case conn := <-p.connections:
			switch conn.GetState() {
			case connectivity.Ready, connectivity.Idle, connectivity.TransientFailure, connectivity.Connecting:
				return conn, nil
			case connectivity.Shutdown:
				_ = conn.Close()
			}
		default:
			// Pool is empty, create a new connection
			break loop
		}
	}

	conn, err := grpc.NewClient(p.address, p.options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create new client: %w", err)
	}

	return conn, nil
}

// Release returns a connection to the pool.
// If the connection is closed or the pool is full, it closes the connection.
func (p *Pool) Release(conn *grpc.ClientConn) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if conn.GetState() != connectivity.Shutdown || len(p.connections) < p.size {
		p.connections <- conn
	} else {
		// Don't put a closed connection back into the pool
		// or pool is full
		_ = conn.Close() // Handle close error if necessary
	}
}

// Close closes all connections in the pool.
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
