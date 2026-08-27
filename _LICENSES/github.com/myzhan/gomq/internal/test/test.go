package test

/*
#cgo !windows pkg-config: libczmq libzmq libsodium
#cgo windows LDFLAGS: -lws2_32 -liphlpapi -lrpcrt4 -lsodium -lzmq -lczmq
#cgo windows CFLAGS: -Wno-pedantic-ms-format -DLIBCZMQ_EXPORTS -DZMQ_DEFINED_STDINT -DLIBCZMQ_EXPORTS

extern void startExternalServer();
extern void startExternalRouter(int port);
*/
import "C"
import "os"

func init() {
	if err := os.Setenv("ZSYS_SIGHANDLER", "false"); err != nil {
		panic(err)
	}
}

// StartExternalServer starts a C service for testing ZMTP compatibility against.
func StartExternalServer() {
	C.startExternalServer()
}

// StartRouter starts a C router for testing ZMTP compatibility against.
func StartRouter(port int) {
	C.startExternalRouter(C.int(port))
}
