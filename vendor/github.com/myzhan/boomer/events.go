package boomer

import "github.com/asaskevich/EventBus"

const (
	EVENT_CONNECTED = "boomer:connected"
	EVENT_SPAWN     = "boomer:spawn"
	EVENT_STOP      = "boomer:stop"
	EVENT_QUIT      = "boomer:quit"
)

// Events is the global event bus instance.
var Events = EventBus.New()
