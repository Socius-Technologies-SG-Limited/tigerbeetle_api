package grpc

import (
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/charithe/timedbuf/v2"
	"github.com/lil5/tigerbeetle_api/config"
	"github.com/stretchr/testify/assert"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func TestParseClusterID(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		id, err := ParseClusterID("0")
		assert.NoError(t, err)
		assert.Equal(t, types.ToUint128(0), id)
	})

	t.Run("small value", func(t *testing.T) {
		id, err := ParseClusterID("12345")
		assert.NoError(t, err)
		assert.Equal(t, types.ToUint128(12345), id)
	})

	t.Run("max uint64", func(t *testing.T) {
		id, err := ParseClusterID("18446744073709551615")
		assert.NoError(t, err)
		assert.Equal(t, types.ToUint128(^uint64(0)), id)
	})

	t.Run("u128 larger than uint64", func(t *testing.T) {
		input := "2141303564190936097871303759524773179"
		id, err := ParseClusterID(input)
		assert.NoError(t, err)
		// Round-trip: convert back to big.Int and compare
		got := id.BigInt()
		expected, ok := new(big.Int).SetString(input, 10)
		assert.True(t, ok)
		assert.Equal(t, 0, got.Cmp(expected), "expected %s, got %s", expected.String(), got.String())
	})

	t.Run("invalid string", func(t *testing.T) {
		_, err := ParseClusterID("not-a-number")
		assert.Error(t, err)
	})

	t.Run("empty string", func(t *testing.T) {
		_, err := ParseClusterID("")
		assert.Error(t, err)
	})
}

func TestHealth(t *testing.T) {
	t.Run("returns nil when TB is healthy", func(t *testing.T) {
		mockClient := new(MockTigerBeetleClient)
		app := &App{TB: mockClient}
		mockClient.On("Nop").Return(nil).Once()
		err := app.Health()
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("returns error when TB is unreachable", func(t *testing.T) {
		mockClient := new(MockTigerBeetleClient)
		app := &App{TB: mockClient}
		mockClient.On("Nop").Return(errors.New("connection refused")).Once()
		err := app.Health()
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestGetRandomBufferCluster(t *testing.T) {
	n := 100000
	cluster := 3
	flushFunc := func(payloads []TimedPayload) {}
	tbufs := make([]*timedbuf.TimedBuf[TimedPayload], cluster)
	for i := range cluster {
		tbufs[i] = timedbuf.New[TimedPayload](cluster, 2*time.Millisecond, flushFunc)
	}

	// set config
	config.Config.BufferCluster = cluster

	// set app
	a := App{
		TBuf:  tbufs[0],
		TBufs: tbufs,
	}

	// test
	t.Run("sync get random buffer", func(t *testing.T) {
		for range n {
			assert.NotPanics(t, func() {
				b := a.getRandomTBuf()
				assert.NotNil(t, b)
			})
		}
	})

	t.Run("async get random buffer", func(t *testing.T) {
		var wg sync.WaitGroup
		for range n {
			wg.Add(1)
			go func() {
				assert.NotPanics(t, func() {
					b := a.getRandomTBuf()
					assert.NotNil(t, b)
				})
				wg.Done()
			}()
		}
		wg.Wait()
	})
}
