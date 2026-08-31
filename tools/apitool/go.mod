module github.com/agent-substrate/substrate/tools/apitool

go 1.27.0

replace github.com/agent-substrate/substrate => ../..

require (
	github.com/agent-substrate/substrate v0.0.0-00010101000000-000000000000
	github.com/bufbuild/protocompile v0.14.1
	github.com/google/go-cmp v0.7.0
	github.com/spf13/cobra v1.10.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260720211330-0afa2a65878a // indirect
	google.golang.org/grpc v1.83.0 // indirect
)
