package boomer

type client interface {
	connect() (err error)
	close()
	recvChannel() chan message
	sendChannel() chan message
	disconnectedChannel() chan bool
}
