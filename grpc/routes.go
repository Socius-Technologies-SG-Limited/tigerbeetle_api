package grpc

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"math/rand/v2"
	"os"
	"strings"

	"github.com/charithe/timedbuf/v2"
	"github.com/lil5/tigerbeetle_api/config"
	"github.com/lil5/tigerbeetle_api/metrics"
	"github.com/lil5/tigerbeetle_api/proto"
	"github.com/samber/lo"
	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
)

var (
	ErrZeroAccounts  = errors.New("no accounts were specified")
	ErrZeroTransfers = errors.New("no transfers were specified")
)

type TimedPayloadResponse struct {
	Replies []*proto.CreateTransfersReplyItem
	Error   error
}
type TimedPayload struct {
	buf       *timedbuf.TimedBuf[TimedPayload]
	c         chan TimedPayloadResponse
	Transfers []tigerbeetle_go.Transfer
}

type App struct {
	proto.UnimplementedTigerBeetleServer

	TB tigerbeetle_go.Client

	TBuf  *timedbuf.TimedBuf[TimedPayload]
	TBufs []*timedbuf.TimedBuf[TimedPayload]
}

func (a *App) getRandomTBuf() *timedbuf.TimedBuf[TimedPayload] {
	if config.Config.BufferCluster > 1 {
		i := rand.IntN(config.Config.BufferCluster - 1)
		return a.TBufs[i]
	} else {
		return a.TBuf
	}
}

func (a *App) Close() {
	for _, b := range a.TBufs {
		b.Close()
	}
	a.TB.Close()
}

const TB_MAX_BATCH_SIZE = 8190

func totalPayloadLen(payloads []TimedPayload) int {
	n := 0
	for _, p := range payloads {
		n += len(p.Transfers)
	}
	return n
}

func ParseClusterID(s string) (tigerbeetle_go.Uint128, error) {
	var id big.Int
	if _, ok := id.SetString(s, 10); !ok {
		return tigerbeetle_go.Uint128{}, errors.New("invalid cluster ID: " + s)
	}
	return tigerbeetle_go.BigIntToUint128(&id), nil
}

func NewApp() *App {
	clusterID, err := ParseClusterID(config.Config.TbClusterID)
	if err != nil {
		slog.Error("invalid TB_CLUSTER_ID", "value", config.Config.TbClusterID, "err", err)
		os.Exit(1)
	}
	tb, err := tigerbeetle_go.NewClient(clusterID, config.Config.TbAddresses)
	if err != nil {
		slog.Error("unable to connect to tigerbeetle", "err", err)
		os.Exit(1)
	}

	var tbuf *timedbuf.TimedBuf[TimedPayload]
	var tbufs []*timedbuf.TimedBuf[TimedPayload]
	if config.Config.IsBuffered {
		tbufs = make([]*timedbuf.TimedBuf[TimedPayload], config.Config.BufferCluster)

		// The maximum batch size is set in the TigerBeetle server. The default is 8190.
		bufSizeFull := float64(config.Config.BufferSize)
		bufSize80 := bufSizeFull * 0.8

		flushFunc := func(payloads []TimedPayload) {
			lenPayloads := float64(len(payloads))

			// Concatenate every payload's transfers in order, splitting into
			// batches no larger than TB_MAX_BATCH_SIZE. Per-payload demux
			// below relies on the input order being preserved.
			totalTransferSize := 0
			indexBatch := 0
			transferBatches := [][]tigerbeetle_go.Transfer{{}}
			for _, payload := range payloads {
				totalTransferSize += len(payload.Transfers)
				if totalTransferSize > TB_MAX_BATCH_SIZE {
					indexBatch++
					transferBatches = append(transferBatches, []tigerbeetle_go.Transfer{})
					totalTransferSize = len(payload.Transfers)
				}
				transferBatches[indexBatch] = append(transferBatches[indexBatch], payload.Transfers...)
			}

			metrics.TotalBufferCount.Inc()
			if lenPayloads == bufSizeFull {
				metrics.TotalBufferContentsFull.Inc()
			} else if lenPayloads >= bufSize80 {
				metrics.TotalBufferContentsGte80.Inc()
			} else {
				metrics.TotalBufferContentsLt80.Inc()
			}

			// In 0.17 each TB call returns a dense result slice (one entry
			// per input transfer). Accumulate across batches so we can demux
			// per-payload at the end.
			allReplies := make([]*proto.CreateTransfersReplyItem, 0, totalPayloadLen(payloads))
			var flushErr error
			for _, transfers := range transferBatches {
				if len(transfers) == 0 {
					continue
				}
				metrics.TotalCreateTransferTx.Add(float64(len(transfers)))
				metrics.TotalTbCreateTransfersCall.Inc()
				if config.Config.IsDryRun {
					continue
				}
				results, err := tb.CreateTransfers(transfers)
				if err != nil {
					flushErr = err
					break
				}
				batchReplies := ResultsToReply(results, transfers)
				for _, r := range batchReplies {
					if r.Status != proto.CreateTransferStatus_TransferCreated {
						metrics.TotalCreateTransferTxErr.Inc()
					}
				}
				allReplies = append(allReplies, batchReplies...)
			}

			// Demux: hand each payload exactly len(payload.Transfers) replies.
			// Under dry-run or a TB error, allReplies may be shorter than the
			// total — give each caller whatever prefix is available (empty if
			// nothing) along with the error.
			cursor := 0
			for _, payload := range payloads {
				want := len(payload.Transfers)
				var slice []*proto.CreateTransfersReplyItem
				if cursor+want <= len(allReplies) {
					slice = allReplies[cursor : cursor+want]
				} else if cursor < len(allReplies) {
					slice = allReplies[cursor:]
				}
				cursor += want
				payload.c <- TimedPayloadResponse{
					Replies: slice,
					Error:   flushErr,
				}
			}
		}
		for i := range config.Config.BufferCluster {
			tbufs[i] = timedbuf.New(config.Config.BufferSize, config.Config.BufferDelay, flushFunc)
		}
		tbuf = tbufs[0]
	}

	app := &App{
		TB:    tb,
		TBuf:  tbuf,
		TBufs: tbufs,
	}
	return app
}

func (s *App) Health() error {
	return s.TB.Nop()
}

func (s *App) GetID(ctx context.Context, in *proto.GetIDRequest) (*proto.GetIDReply, error) {
	return &proto.GetIDReply{Id: tigerbeetle_go.ID().String()}, nil
}

func (s *App) CreateAccounts(ctx context.Context, in *proto.CreateAccountsRequest) (*proto.CreateAccountsReply, error) {
	if len(in.Accounts) == 0 {
		return nil, ErrZeroAccounts
	}
	accounts := []tigerbeetle_go.Account{}
	for _, inAccount := range in.Accounts {
		id, err := HexStringToUint128(inAccount.Id)
		if err != nil {
			return nil, err
		}
		userData128, err := tigerbeetle_go.HexStringToUint128(inAccount.UserData128)
		if err != nil {
			return nil, err
		}
		flags := tigerbeetle_go.AccountFlags{}
		if inAccount.Flags != nil {
			flags.Linked = lo.FromPtrOr(inAccount.Flags.Linked, false)
			flags.DebitsMustNotExceedCredits = lo.FromPtrOr(inAccount.Flags.DebitsMustNotExceedCredits, false)
			flags.CreditsMustNotExceedDebits = lo.FromPtrOr(inAccount.Flags.CreditsMustNotExceedDebits, false)
			flags.History = lo.FromPtrOr(inAccount.Flags.History, false)
		}
		accounts = append(accounts, tigerbeetle_go.Account{
			ID:             *id,
			DebitsPending:  tigerbeetle_go.ToUint128(uint64(inAccount.DebitsPending)),
			DebitsPosted:   tigerbeetle_go.ToUint128(uint64(inAccount.DebitsPosted)),
			CreditsPending: tigerbeetle_go.ToUint128(uint64(inAccount.CreditsPending)),
			CreditsPosted:  tigerbeetle_go.ToUint128(uint64(inAccount.CreditsPosted)),
			UserData128:    userData128,
			UserData64:     uint64(inAccount.UserData64),
			UserData32:     uint32(inAccount.UserData32),
			Ledger:         uint32(inAccount.Ledger),
			Code:           uint16(inAccount.Code),
			Flags:          flags.ToUint16(),
		})
	}

	metrics.TotalTbCreateAccountsCall.Inc()
	metrics.TotalCreateAccountsTx.Add(float64(len(accounts)))
	results, err := s.TB.CreateAccounts(accounts)
	if err != nil {
		return nil, err
	}

	resArr := AccountResultsToReply(results, accounts)
	for _, r := range resArr {
		if r.Status != proto.CreateAccountStatus_AccountCreated {
			metrics.TotalCreateAccountsTxErr.Inc()
		}
	}
	return &proto.CreateAccountsReply{
		Results: resArr,
	}, nil
}

func (s *App) CreateTransfers(ctx context.Context, in *proto.CreateTransfersRequest) (*proto.CreateTransfersReply, error) {
	if len(in.Transfers) == 0 {
		return nil, ErrZeroTransfers
	}
	transfers := []tigerbeetle_go.Transfer{}
	for _, inTransfer := range in.Transfers {
		id, err := HexStringToUint128(inTransfer.Id)
		if err != nil {
			return nil, err
		}
		flags := tigerbeetle_go.TransferFlags{}
		if inTransfer.TransferFlags != nil {
			flags.Linked = lo.FromPtrOr(inTransfer.TransferFlags.Linked, false)
			flags.Pending = lo.FromPtrOr(inTransfer.TransferFlags.Pending, false)
			flags.PostPendingTransfer = lo.FromPtrOr(inTransfer.TransferFlags.PostPendingTransfer, false)
			flags.VoidPendingTransfer = lo.FromPtrOr(inTransfer.TransferFlags.VoidPendingTransfer, false)
			flags.BalancingDebit = lo.FromPtrOr(inTransfer.TransferFlags.BalancingDebit, false)
			flags.BalancingCredit = lo.FromPtrOr(inTransfer.TransferFlags.BalancingCredit, false)
			flags.ClosingDebit = lo.FromPtrOr(inTransfer.TransferFlags.ClosingDebit, false)
			flags.ClosingCredit = lo.FromPtrOr(inTransfer.TransferFlags.ClosingCredit, false)
			flags.Imported = lo.FromPtrOr(inTransfer.TransferFlags.Imported, false)
		}
		debitAccountID, err := HexStringToUint128(inTransfer.DebitAccountId)
		if err != nil {
			return nil, err
		}
		creditAccountID, err := HexStringToUint128(inTransfer.CreditAccountId)
		if err != nil {
			return nil, err
		}
		pendingID, err := HexStringToUint128(lo.FromPtrOr(inTransfer.PendingId, ""))
		if err != nil {
			return nil, err
		}
		userData128, err := tigerbeetle_go.HexStringToUint128(inTransfer.UserData128)
		if err != nil {
			return nil, err
		}
		transfers = append(transfers, tigerbeetle_go.Transfer{
			ID:              *id,
			DebitAccountID:  *debitAccountID,
			CreditAccountID: *creditAccountID,
			Amount:          tigerbeetle_go.ToUint128(uint64(inTransfer.Amount)),
			PendingID:       *pendingID,
			UserData128:     userData128,
			UserData64:      uint64(inTransfer.UserData64),
			UserData32:      uint32(inTransfer.UserData32),
			Timeout:         0,
			Ledger:          uint32(inTransfer.Ledger),
			Code:            uint16(inTransfer.Code),
			Flags:           flags.ToUint16(),
			Timestamp:       0,
		})
	}

	var err error
	var replies []*proto.CreateTransfersReplyItem
	if config.Config.IsBuffered {
		buf := s.getRandomTBuf()
		c := make(chan TimedPayloadResponse)
		buf.Put(TimedPayload{
			c:         c,
			buf:       buf,
			Transfers: transfers,
		})
		res := <-c
		replies = res.Replies
		err = res.Error
	} else {
		metrics.TotalTbCreateTransfersCall.Inc()
		metrics.TotalCreateTransferTx.Add(float64(len(transfers)))
		if !config.Config.IsDryRun {
			var results []tigerbeetle_go.CreateTransferResult
			results, err = s.TB.CreateTransfers(transfers)
			if err == nil {
				replies = ResultsToReply(results, transfers)
				for _, r := range replies {
					if r.Status != proto.CreateTransferStatus_TransferCreated {
						metrics.TotalCreateTransferTxErr.Inc()
					}
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return &proto.CreateTransfersReply{
		Results: replies,
	}, nil
}

func (s *App) LookupAccounts(ctx context.Context, in *proto.LookupAccountsRequest) (*proto.LookupAccountsReply, error) {
	if len(in.AccountIds) == 0 {
		return nil, ErrZeroAccounts
	}
	ids := []tigerbeetle_go.Uint128{}
	for _, inID := range in.AccountIds {
		id, err := HexStringToUint128(inID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, *id)
	}

	metrics.TotalTbLookupAccountsCall.Inc()
	res, err := s.TB.LookupAccounts(ids)
	if err != nil {
		return nil, err
	}

	pAccounts := lo.Map(res, func(a tigerbeetle_go.Account, _ int) *proto.Account {
		return AccountToProtoAccount(a)
	})
	return &proto.LookupAccountsReply{Accounts: pAccounts}, nil
}

func (s *App) LookupTransfers(ctx context.Context, in *proto.LookupTransfersRequest) (*proto.LookupTransfersReply, error) {
	if len(in.TransferIds) == 0 {
		return nil, ErrZeroTransfers
	}
	ids := []tigerbeetle_go.Uint128{}
	for _, inID := range in.TransferIds {
		id, err := HexStringToUint128(inID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, *id)
	}

	metrics.TotalTbLookupTransfersCall.Inc()
	res, err := s.TB.LookupTransfers(ids)
	if err != nil {
		return nil, err
	}

	pTransfers := lo.Map(res, func(a tigerbeetle_go.Transfer, _ int) *proto.Transfer {
		return TransferToProtoTransfer(a)
	})
	return &proto.LookupTransfersReply{Transfers: pTransfers}, nil
}

func (s *App) GetAccountTransfers(ctx context.Context, in *proto.GetAccountTransfersRequest) (*proto.GetAccountTransfersReply, error) {
	if in.Filter.AccountId == "" {
		return nil, ErrZeroAccounts
	}
	tbFilter, err := AccountFilterFromProtoToTigerbeetle(in.Filter)
	if err != nil {
		return nil, err
	}
	metrics.TotalTbGetAccountTransfersCall.Inc()
	res, err := s.TB.GetAccountTransfers(*tbFilter)
	if err != nil {
		return nil, err
	}

	pTransfers := lo.Map(res, func(v tigerbeetle_go.Transfer, _ int) *proto.Transfer {
		return TransferToProtoTransfer(v)
	})
	return &proto.GetAccountTransfersReply{Transfers: pTransfers}, nil
}

func (s *App) GetAccountBalances(ctx context.Context, in *proto.GetAccountBalancesRequest) (*proto.GetAccountBalancesReply, error) {
	if in.Filter.AccountId == "" {
		return nil, ErrZeroAccounts
	}
	tbFilter, err := AccountFilterFromProtoToTigerbeetle(in.Filter)
	if err != nil {
		return nil, err
	}
	metrics.TotalTbGetAccountBalancesCall.Inc()
	res, err := s.TB.GetAccountBalances(*tbFilter)
	if err != nil {
		return nil, err
	}

	pBalances := lo.Map(res, func(v tigerbeetle_go.AccountBalance, _ int) *proto.AccountBalance {
		return AccountBalanceFromTigerbeetleToProto(v)
	})
	return &proto.GetAccountBalancesReply{AccountBalances: pBalances}, nil
}

func (s *App) QueryTransfers(ctx context.Context, in *proto.QueryTransfersRequest) (*proto.QueryTransfersReply, error) {
	if in.Filter == nil {
		return nil, errors.New("filter is required")
	}

	tbFilter, err := QueryFilterFromProtoToTigerbeetle(in.Filter)
	if err != nil {
		if in.Filter.UserData128 != nil && strings.Contains(err.Error(), "hex") {
			return nil, errors.New("invalid UserData128: " + err.Error())
		}
		return nil, err
	}

	metrics.TotalTbQueryTransfersCall.Inc()
	res, err := s.TB.QueryTransfers(*tbFilter)
	if err != nil {
		return nil, err
	}

	pTransfers := lo.Map(res, func(v tigerbeetle_go.Transfer, _ int) *proto.Transfer {
		return TransferToProtoTransfer(v)
	})
	return &proto.QueryTransfersReply{Transfers: pTransfers}, nil
}

func (s *App) QueryAccounts(ctx context.Context, in *proto.QueryAccountsRequest) (*proto.QueryAccountsReply, error) {
	if in.Filter == nil {
		return nil, errors.New("filter is required")
	}

	tbFilter, err := QueryFilterFromProtoToTigerbeetle(in.Filter)
	if err != nil {
		if in.Filter.UserData128 != nil && strings.Contains(err.Error(), "hex") {
			return nil, errors.New("invalid UserData128: " + err.Error())
		}
		return nil, err
	}

	metrics.TotalTbQueryAccountsCall.Inc()
	res, err := s.TB.QueryAccounts(*tbFilter)
	if err != nil {
		return nil, err
	}

	pAccounts := lo.Map(res, func(v tigerbeetle_go.Account, _ int) *proto.Account {
		return AccountToProtoAccount(v)
	})
	return &proto.QueryAccountsReply{Accounts: pAccounts}, nil
}
