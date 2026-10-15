// transportprobe is bounded test tooling, never a database or SDK substitute.
// It is built with CGO_ENABLED=0 and runs inside the five owned FDB cgroups.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const payloadBytes = 64
const maxClients = 4

type sample struct {
	Source, Target, TokenSHA256 string
	Ordinal                     int
	Nanos                       int64
	Bytes                       int
}

func original(token string) ([payloadBytes]byte, error) {
	var out [payloadBytes]byte
	raw, err := hex.DecodeString(token)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != token || bytes.Equal(raw, make([]byte, 32)) {
		return out, errors.New("exact nonzero run token required")
	}
	copy(out[:], []byte(token))
	return out, nil
}
func exchange(conn net.Conn, payload [payloadBytes]byte, echo bool) error {
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	var received [payloadBytes]byte
	if echo {
		if _, err := io.ReadFull(conn, received[:]); err != nil {
			return err
		}
		if received != payload {
			return errors.New("foreign probe original")
		}
	}
	if n, err := conn.Write(payload[:]); err != nil {
		return err
	} else if n != len(payload) {
		return io.ErrShortWrite
	}
	if !echo {
		if _, err := io.ReadFull(conn, received[:]); err != nil {
			return err
		}
		if received != payload {
			return errors.New("probe bytes changed")
		}
	}
	return nil
}
func serve(ctx context.Context, address string, payload [payloadBytes]byte) error {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", address)
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = l.Close() })
	defer stop()
	defer l.Close()
	var group sync.WaitGroup
	defer group.Wait()
	slots := make(chan struct{}, maxClients)
	if err := json.NewEncoder(os.Stdout).Encode(struct {
		Ready   string
		PID     int
		Address string
	}{"owned-tcp-echo", os.Getpid(), l.Addr().String()}); err != nil {
		return err
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case slots <- struct{}{}:
		default:
			_ = conn.Close()
			return errors.New("probe connection budget exhausted")
		}
		group.Go(func() {
			defer func() { <-slots }()
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
			defer stop()
			_ = exchange(conn, payload, true) // Caller must receive exact echo or fail.
		})
	}
}
func measure(ctx context.Context, source, target string, payload [payloadBytes]byte, out io.Writer) error {
	digest := sha256.Sum256(payload[:])
	for ordinal := range 9 {
		start := time.Now()
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := (&net.Dialer{Timeout: 3 * time.Second, LocalAddr: &net.TCPAddr{IP: net.ParseIP(source)}}).DialContext(probeCtx, "tcp4", net.JoinHostPort(target, "4501"))
		if err != nil {
			cancel()
			return err
		}
		stop := context.AfterFunc(probeCtx, func() { _ = conn.Close() })
		err = exchange(conn, payload, false)
		stop()
		err = errors.Join(err, conn.Close())
		cancel()
		if err != nil {
			return err
		}
		if err := json.NewEncoder(out).Encode(sample{source, target, hex.EncodeToString(digest[:]), ordinal, time.Since(start).Nanoseconds(), payloadBytes}); err != nil {
			return err
		}
	}
	return nil
}
func run(args []string) error {
	if len(args) != 5 || args[0] != "serve" && args[0] != "measure" {
		return errors.New("serve|measure SOURCE TARGET TOKEN DEADLINE required")
	}
	payload, err := original(args[3])
	if err != nil {
		return err
	}
	deadline, err := strconv.ParseInt(args[4], 10, 64)
	if err != nil || deadline <= time.Now().UnixNano() {
		return errors.New("original finite deadline required")
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, deadline))
	defer cancel()
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if net.ParseIP(args[1]).To4() == nil || net.ParseIP(args[2]).To4() == nil {
		return errors.New("exact owned IPv4 required")
	}
	if args[0] == "serve" {
		return serve(ctx, net.JoinHostPort(args[1], "4501"), payload)
	}
	return measure(ctx, args[1], args[2], payload, os.Stdout)
}
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
