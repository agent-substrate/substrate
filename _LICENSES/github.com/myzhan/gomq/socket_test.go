package gomq

import (
	"bytes"
	"net"
	"testing"

	"github.com/myzhan/gomq/internal/test"
	"github.com/myzhan/gomq/zmtp"
)

func TestNewClient(t *testing.T) {
	var addr net.Addr
	var err error

	go func() {
		client := NewClient(zmtp.NewSecurityNull())
		err = client.Connect("tcp://127.0.0.1:9999")
		if err != nil {
			t.Error(err)
		}

		err := client.Send([]byte("HELLO"))
		if err != nil {
			t.Error(err)
		}

		msg, _ := client.Recv()
		if want, got := 0, bytes.Compare([]byte("WORLD"), msg); want != got {
			t.Errorf("want %v, got %v", want, got)
		}

		t.Logf("client received: %q", string(msg))

		err = client.Send([]byte("GOODBYE"))
		if err != nil {
			t.Error(err)
		}

		client.Close()
	}()

	server := NewServer(zmtp.NewSecurityNull())

	addr, err = server.Bind("tcp://127.0.0.1:9999")
	if err != nil {
		t.Fatal(err)
	}

	if want, got := "127.0.0.1:9999", addr.String(); want != got {
		t.Fatalf("want %q, got %q", want, got)
	}

	if err != nil {
		t.Fatal(err)
	}

	msg, _ := server.Recv()

	if want, got := 0, bytes.Compare([]byte("HELLO"), msg); want != got {
		t.Fatalf("want %v, got %v", want, got)
	}

	t.Logf("server received: %q", string(msg))

	server.Send([]byte("WORLD"))

	msg, err = server.Recv()
	if err != nil {
		t.Fatal(err)
	}

	if want, got := 0, bytes.Compare([]byte("GOODBYE"), msg); want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	t.Logf("server received: %q", string(msg))

	server.Close()
}

func TestExternalServer(t *testing.T) {
	go test.StartExternalServer()

	client := NewClient(zmtp.NewSecurityNull())
	err := client.Connect("tcp://127.0.0.1:31337")
	if err != nil {
		t.Fatal(err)
	}

	err = client.Send([]byte("HELLO"))
	if err != nil {
		t.Fatal(err)
	}

	msg, _ := client.Recv()

	if want, got := 0, bytes.Compare([]byte("WORLD"), msg); want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	t.Logf("client received: %q", string(msg))

	client.Close()
}

func TestPushPull(t *testing.T) {
	var addr net.Addr
	var err error

	go func() {
		pull := NewPull(zmtp.NewSecurityNull())
		defer pull.Close()
		err = pull.Connect("tcp://127.0.0.1:12345")
		if err != nil {
			t.Fatal(err)
		}

		msg, err := pull.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if want, got := 0, bytes.Compare([]byte("HELLO"), msg); want != got {
			t.Fatalf("want %v, got %v", want, got)
		}

		t.Logf("pull received: %q", string(msg))

		err = pull.Send([]byte("GOODBYE"))
		if err != nil {
			t.Fatal(err)
		}

		pull.Close()
	}()

	push := NewPush(zmtp.NewSecurityNull())
	defer push.Close()

	addr, err = push.Bind("tcp://127.0.0.1:12345")
	if err != nil {
		t.Fatal(err)
	}

	if want, got := "127.0.0.1:12345", addr.String(); want != got {
		t.Fatalf("want %q, got %q", want, got)
	}

	if err != nil {
		t.Fatal(err)
	}

	push.Send([]byte("HELLO"))

	msg, err := push.Recv()
	if err != nil {
		t.Fatal(err)
	}

	if want, got := 0, bytes.Compare([]byte("GOODBYE"), msg); want != got {
		t.Fatalf("want %v, got %v (%v)", want, got, msg)
	}

	t.Logf("push received: %q", string(msg))

	push.Close()
}

func TestPullPush(t *testing.T) {
	port := "19001"
	var addr net.Addr
	var err error

	go func() {
		push := NewPush(zmtp.NewSecurityNull())
		defer push.Close()
		err = push.Connect("tcp://127.0.0.1:" + port)
		if err != nil {
			t.Fatal(err)
		}

		msg, err := push.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if want, got := 0, bytes.Compare([]byte("HELLO"), msg); want != got {
			t.Fatalf("want %v, got %v", want, got)
		}

		t.Logf("push received: %q", string(msg))

		err = push.Send([]byte("GOODBYE"))
		if err != nil {
			t.Fatal(err)
		}

		push.Close()
	}()

	pull := NewPull(zmtp.NewSecurityNull())
	defer pull.Close()

	addr, err = pull.Bind("tcp://127.0.0.1:" + port)
	if err != nil {
		t.Fatal(err)
	}

	if want, got := "127.0.0.1:"+port, addr.String(); want != got {
		t.Fatalf("want %q, got %q", want, got)
	}

	if err != nil {
		t.Fatal(err)
	}

	pull.Send([]byte("HELLO"))

	msg, err := pull.Recv()
	if err != nil {
		t.Fatal(err)
	}

	if want, got := 0, bytes.Compare([]byte("GOODBYE"), msg); want != got {
		t.Fatalf("want %v, got %v (%v)", want, got, msg)
	}

	t.Logf("pull received: %q", string(msg))

	pull.Close()
}

func TestDealerExtRouter(t *testing.T) {

	go test.StartRouter(31340)

	dealer := NewDealer(zmtp.NewSecurityNull(), "dealer-id")
	err := dealer.Connect("tcp://127.0.0.1:31340")
	if err != nil {
		t.Fatalf("could not connect: %v", err)
	}

	err = dealer.SendMultipart([][]byte{[]byte("HELLO")})
	if err != nil {
		t.Fatalf("could not send message: %v", err)
	}

	msg, err := dealer.Recv()
	if err != nil {
		t.Fatalf("could not receive message: %v", err)
	}

	if want, got := 0, bytes.Compare([]byte("WORLD"), msg); want != got {
		t.Fatalf("want %v, got %v", want, got)
	}

	t.Logf("dealer received: %q", string(msg))

	dealer.Close()
}

func TestBadEndpointError(t *testing.T) {
	client := NewClient(zmtp.NewSecurityNull())
	err := client.Connect("ipc://@/not-yet-implemented")
	if err == nil {
		t.Error("ipc protocol MUST raise error")
	}
}
