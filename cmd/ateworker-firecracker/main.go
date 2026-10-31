package main

import (
	"fmt"
	"log"

	"github.com/spf13/pflag"
	"go.ate.dev/substrate/internal/ateworker/pid1"
	"go.ate.dev/substrate/internal/version"
)

var (
	podUID      = pflag.String("pod-uid", "", "The UID of the current pod")
	podIPs      = pflag.String("pod-ips", "", "The comma-separated IPs of the current pod")
	showVersion = pflag.Bool("version", false, "Print version and exit.")
)

func main() {
	pid1.RunIfPID1()
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}

	log.Printf("starting ateworker firecracker ")
	log.Printf("pod uid: %s", *podUID)
	log.Printf("pod ips: %s", *podIPs)
}
