package influxdb_test

import (
	"net"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb-client"
)

const MAX_UDP_PAYLOAD = 64 * 1024

func TestUDPWriter(t *testing.T) {
	// Listen on a random ephemeral port.
	saddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.ListenUDP("udp", saddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	addr := conn.LocalAddr()

	done := make(chan struct{})
	go func() {
		defer close(done)

		data := make([]byte, MAX_UDP_PAYLOAD)
		_, _, err := conn.ReadFromUDP(data)
		if err != nil {
			t.Error(err)
		}
	}()

	// Create the UDP writer.
	w, err := influxdb.NewUDPWriter(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	now := time.Now()
	pt := influxdb.NewPoint("cpu", influxdb.Value(2.0), now)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for i := 0; i < 10; i++ {
		if err := w.Write(pt); err != nil {
			t.Fatal(err)
		}

		select {
		case <-ticker.C:
		case <-done:
			break
		}
	}

	// Check if the packet was received.
	select {
	case <-done:
	default:
		t.Errorf("timeout while waiting for udp packet")
	}
}
