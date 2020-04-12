package fasthttp

import (
	"log"
	"net"
	"reflect"
	"sync"
	"syscall"
	"golang.org/x/sys/unix"
)

//https://github.com/eranyanay/1m-go-websockets/blob/master/4_optimize_gobwas/epoll.go

var epoller *epoll

type epoll struct {
	fd          int
	connections map[int]*wrapStateConn
	lock        *sync.RWMutex
}

type wrapStateConn struct {
	net.Conn
	poll        bool
}

func initEpoller(wp *workerPool) error {
	var err error
	epoller, err = MkEpoll()
	if err != nil {
		return err
	}
	go poll(wp)
	return nil
}

func MkEpoll() (*epoll, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &epoll{
		fd:          fd,
		lock:        &sync.RWMutex{},
		connections: make(map[int]*wrapStateConn),
	}, nil
}

func (e *epoll) Add(conn net.Conn) error {
	fd := tcpFD(conn)

	e.lock.Lock()
	defer e.lock.Unlock()
	c, ok := e.connections[fd]

	if !ok {
		// Extract file descriptor associated with the connection
		event := &unix.EpollEvent{Events: unix.POLLIN | unix.POLLHUP |unix.EPOLLERR, Fd: int32(fd)}
		err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, event)
		if err != nil {
			return err
		}
		log.Printf("Success to add conn %s with fd %d", conn.RemoteAddr().String(), fd)
		e.connections[fd] = &wrapStateConn{Conn: conn, poll: true}
		if len(e.connections)%100 == 0 {
			log.Printf("Total number of connections: %v", len(e.connections))
		}
	} else {
		c.poll = true
		log.Printf("Success to add conn %s with fd %d", conn.RemoteAddr().String(), fd)
	}
	return nil
}

func (e *epoll) Remove(conn net.Conn) error {
	fd := tcpFD(conn)
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}
	log.Printf("Success to remove conn %s with fd %d", conn.RemoteAddr().String(), fd)
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.connections, fd)
	if len(e.connections)%100 == 0 {
		log.Printf("Total number of connections: %v", len(e.connections))
	}
	return nil
}

func (e *epoll) Wait() ([]net.Conn, error) {
	events := make([]unix.EpollEvent, 512)
	n, err := unix.EpollWait(e.fd, events, 512)
	if err != nil {
		return nil, err
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	var connections []net.Conn
	for i := 0; i < n; i++ {
		conn := e.connections[int(events[i].Fd)]
		if conn.poll {
			connections = append(connections, conn.Conn)
			conn.poll = false
		}
	}
	return connections, nil
}

func poll(wp *workerPool) {
	for {
		connections, err := epoller.Wait()
		if err != nil {
			log.Printf("Failed to epoll wait %v", err)
			continue
		}

		for _, conn := range connections {
			log.Printf("Success to wait conn %s", conn.RemoteAddr().String())
			if err := epoller.Remove(conn); err != nil {
				log.Printf("Failed to remove %v", err)
			}
			if !wp.Serve(conn) {
				log.Printf("Failed to serve conn %s", conn.RemoteAddr().String())
			}
		}
	}
}

func tcpFD(conn net.Conn) int {
	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")

	return int(pfdVal.FieldByName("Sysfd").Int())
}