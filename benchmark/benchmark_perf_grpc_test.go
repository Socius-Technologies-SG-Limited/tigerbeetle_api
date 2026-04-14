package benchmark

import (
	"benchmark/proto"
	"context"
	"errors"
	"fmt"
	"testing"

	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func BenchmarkGrpcTest(b *testing.B) {
	close := tbApiStart([]string{
		"PORT=50054",
		"TB_ADDRESSES=3033",
		"TB_CLUSTER_ID=0",
		"USE_GRPC=true",
		"GRPC_REFLECTION=true",
		"CLIENT_COUNT=1",
		"MODE=production",
	})
	defer close()

	conn, err := grpc.NewClient("localhost:50054", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Error(err)
		return
	}
	defer conn.Close()
	c := proto.NewTigerBeetleClient(conn)

	b.ResetTimer()
	for range b.N {
		res, err := c.CreateTransfers(context.Background(), &proto.CreateTransfersRequest{
			Transfers: []*proto.Transfer{
				{
					Id:              tigerbeetle_go.ID().String(),
					DebitAccountId:  tigerbeetle_go.Uint128{2}.String(),
					CreditAccountId: tigerbeetle_go.Uint128{3}.String(),
					Amount:          1,
					Ledger:          999,
					Code:            1,
					TransferFlags:   &proto.TransferFlags{},
					UserData128:     tigerbeetle_go.Uint128{0}.String(),
					UserData64:      0,
					UserData32:      0,
				},
				{
					Id:              tigerbeetle_go.ID().String(),
					DebitAccountId:  tigerbeetle_go.Uint128{2}.String(),
					CreditAccountId: tigerbeetle_go.Uint128{3}.String(),
					Amount:          1,
					Ledger:          999,
					Code:            1,
					TransferFlags:   &proto.TransferFlags{},
					UserData128:     tigerbeetle_go.Uint128{0}.String(),
					UserData64:      0,
					UserData32:      0,
				},
			},
		})
		if err == nil {
			if res == nil {
				err = errors.New("Grpc result is nil")
			} else {
				// 0.17: results are dense, one per input. The benchmark always
				// submits 2 transfers; success means both statuses are the
				// zero value (TransferCreated after proto mapping).
				for i, r := range res.Results {
					if r.Status != proto.CreateTransferStatus_TransferCreated {
						err = fmt.Errorf("transfers[%d] status %v, expected TransferCreated", i, r.Status)
						break
					}
				}
			}
		}
		if err != nil {
			b.Error(err)
			return
		}
	}
}
