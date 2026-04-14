package benchmark

import (
	"testing"

	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
)

func BenchmarkTbClient(b *testing.B) {
	tbAddresses := []string{"3033"}
	clusterUint128 := tigerbeetle_go.Uint128{0}

	// Start the tb client
	tb, err := tigerbeetle_go.NewClient(clusterUint128, tbAddresses)
	if err != nil {
		b.Error(err)
	}
	defer tb.Close()

	b.ResetTimer()
	for range b.N {
		f := tigerbeetle_go.TransferFlags{}
		res, err := tb.CreateTransfers([]tigerbeetle_go.Transfer{

			{
				ID:              tigerbeetle_go.ID(),
				DebitAccountID:  tigerbeetle_go.Uint128{2},
				CreditAccountID: tigerbeetle_go.Uint128{3},
				Amount:          tigerbeetle_go.Uint128{1},
				Ledger:          999,
				Code:            1,
				Flags:           f.ToUint16(),
				UserData128:     tigerbeetle_go.Uint128{0},
				UserData64:      0,
				UserData32:      0,
			},
			{
				ID:              tigerbeetle_go.ID(),
				DebitAccountID:  tigerbeetle_go.Uint128{2},
				CreditAccountID: tigerbeetle_go.Uint128{3},
				Amount:          tigerbeetle_go.Uint128{1},
				Ledger:          999,
				Code:            1,
				Flags:           f.ToUint16(),
				UserData128:     tigerbeetle_go.Uint128{0},
				UserData64:      0,
				UserData32:      0,
			},
		})

		if err != nil {
			b.Error(err)
		}
		if len(res) != 0 {
			b.Error("Results gt 0", res)
		}
	}
}
