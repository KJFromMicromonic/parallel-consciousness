package bus_test

import (
	"testing"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/bustest"
)

func TestInMemoryConformance(t *testing.T) {
	bustest.Run(t, func(t *testing.T) bus.Bus {
		return bus.NewInMemory(64)
	})
}
