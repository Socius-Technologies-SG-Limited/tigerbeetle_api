package grpc

import (
	"log/slog"

	"github.com/lil5/tigerbeetle_api/proto"
	"github.com/samber/lo"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func AccountToProtoAccount(tbAccount types.Account) *proto.Account {
	tbFlags := tbAccount.AccountFlags()
	pFlags := proto.AccountFlags{
		Linked:                     lo.ToPtr(tbFlags.Linked),
		DebitsMustNotExceedCredits: lo.ToPtr(tbFlags.DebitsMustNotExceedCredits),
		CreditsMustNotExceedDebits: lo.ToPtr(tbFlags.CreditsMustNotExceedDebits),
		History:                    lo.ToPtr(tbFlags.History),
		Closed:                     lo.ToPtr(tbFlags.Closed),
	}
	return &proto.Account{
		Id:             tbAccount.ID.String(),
		DebitsPending:  lo.ToPtr(tbAccount.DebitsPending.BigInt()).Uint64(),
		DebitsPosted:   lo.ToPtr(tbAccount.DebitsPosted.BigInt()).Uint64(),
		CreditsPending: lo.ToPtr(tbAccount.CreditsPending.BigInt()).Uint64(),
		CreditsPosted:  lo.ToPtr(tbAccount.CreditsPosted.BigInt()).Uint64(),
		UserData128:    tbAccount.UserData128.String(),
		UserData64:     tbAccount.UserData64,
		UserData32:     tbAccount.UserData32,
		Ledger:         tbAccount.Ledger,
		Code:           uint32(tbAccount.Code),
		Flags:          &pFlags,
		Timestamp:      uint64(tbAccount.Timestamp),
	}
}

func TransferToProtoTransfer(tbTransfer types.Transfer) *proto.Transfer {
	tbFlags := tbTransfer.TransferFlags()
	pFlags := &proto.TransferFlags{
		Linked:              lo.ToPtr(tbFlags.Linked),
		Pending:             lo.ToPtr(tbFlags.Pending),
		PostPendingTransfer: lo.ToPtr(tbFlags.PostPendingTransfer),
		VoidPendingTransfer: lo.ToPtr(tbFlags.VoidPendingTransfer),
		BalancingDebit:      lo.ToPtr(tbFlags.BalancingDebit),
		BalancingCredit:     lo.ToPtr(tbFlags.BalancingCredit),
		ClosingDebit:        lo.ToPtr(tbFlags.ClosingDebit),
		ClosingCredit:       lo.ToPtr(tbFlags.ClosingCredit),
		Imported:            lo.ToPtr(tbFlags.Imported),
	}
	var pendingId string
	emptyUint128 := types.Uint128{}
	if tbTransfer.PendingID != emptyUint128 {
		pendingId = tbTransfer.PendingID.String()
	}
	return &proto.Transfer{
		Id:              tbTransfer.ID.String(),
		DebitAccountId:  tbTransfer.DebitAccountID.String(),
		CreditAccountId: tbTransfer.CreditAccountID.String(),
		Amount:          lo.ToPtr(tbTransfer.Amount.BigInt()).Int64(),
		PendingId:       lo.If[*string](pendingId == "", nil).Else(&pendingId),
		UserData128:     tbTransfer.UserData128.String(),
		UserData64:      tbTransfer.UserData64,
		UserData32:      tbTransfer.UserData32,
		Ledger:          tbTransfer.Ledger,
		Code:            uint32(tbTransfer.Code),
		TransferFlags:   pFlags,
		Timestamp:       &tbTransfer.Timestamp,
	}
}

func AccountFilterFromProtoToTigerbeetle(pAccountFilter *proto.AccountFilter) (*types.AccountFilter, error) {
	accountID, err := HexStringToUint128(pAccountFilter.AccountId)
	if err != nil {
		return nil, err
	}

	var tbFlags types.AccountFilterFlags
	if pAccountFilter.Flags != nil {
		tbFlags = types.AccountFilterFlags{
			Debits:   lo.FromPtrOr(pAccountFilter.Flags.Debits, false),
			Credits:  lo.FromPtrOr(pAccountFilter.Flags.Credits, false),
			Reversed: lo.FromPtrOr(pAccountFilter.Flags.Reversed, false),
		}
	}

	return &types.AccountFilter{
		AccountID:    *accountID,
		TimestampMin: uint64(lo.FromPtrOr(pAccountFilter.TimestampMin, 0)),
		TimestampMax: uint64(lo.FromPtrOr(pAccountFilter.TimestampMax, 0)),
		Limit:        uint32(pAccountFilter.Limit),
		Flags:        tbFlags.ToUint32(),
	}, nil
}

func AccountBalanceFromTigerbeetleToProto(tbBalance types.AccountBalance) *proto.AccountBalance {
	return &proto.AccountBalance{
		DebitsPending:  lo.ToPtr(tbBalance.DebitsPending.BigInt()).Uint64(),
		DebitsPosted:   lo.ToPtr(tbBalance.DebitsPosted.BigInt()).Uint64(),
		CreditsPending: lo.ToPtr(tbBalance.CreditsPending.BigInt()).Uint64(),
		CreditsPosted:  lo.ToPtr(tbBalance.CreditsPosted.BigInt()).Uint64(),
		Timestamp:      tbBalance.Timestamp,
	}
}

func HexStringToUint128(hex string) (*types.Uint128, error) {
	if hex == "" {
		return &types.Uint128{0}, nil
	}

	res, err := types.HexStringToUint128(hex)
	if err != nil {
		slog.Error("hex string to Uint128 failed", "hex", hex, "error", err)
		return nil, err
	}
	return &res, nil

}

func ResultsToReply(results []types.TransferEventResult, transfers []types.Transfer, err error) []*proto.CreateTransfersReplyItem {
	replies := make([]*proto.CreateTransfersReplyItem, 0, len(results))
	for _, r := range results {
		replies = append(replies, &proto.CreateTransfersReplyItem{
			Index:  int32(r.Index),
			Result: proto.CreateTransferResult(r.Result),
			Id:     transfers[r.Index].ID.String(),
		})
	}
	return replies
}

func QueryFilterFromProtoToTigerbeetle(pFilter *proto.QueryFilter) (*types.QueryFilter, error) {
	if pFilter == nil {
		return nil, nil
	}

	var userData128 types.Uint128
	if pFilter.UserData128 != nil && *pFilter.UserData128 != "" {
		var err error
		userData128, err = types.HexStringToUint128(*pFilter.UserData128)
		if err != nil {
			slog.Error("invalid UserData128 hex string", "hex", *pFilter.UserData128, "error", err)
			return nil, err
		}
	}

	var flags types.QueryFilterFlags
	if pFilter.Flags != nil {
		flags = types.QueryFilterFlags{
			Reversed: lo.FromPtrOr(pFilter.Flags.Reversed, false),
		}
	}

	return &types.QueryFilter{
		UserData128:  userData128,
		UserData64:   lo.FromPtrOr(pFilter.UserData64, 0),
		UserData32:   lo.FromPtrOr(pFilter.UserData32, 0),
		Code:         uint16(lo.FromPtrOr(pFilter.Code, 0)),
		Ledger:       lo.FromPtrOr(pFilter.Ledger, 0),
		TimestampMin: lo.FromPtrOr(pFilter.TimestampMin, 0),
		TimestampMax: lo.FromPtrOr(pFilter.TimestampMax, 0),
		Limit:        pFilter.Limit,
		Flags:        flags.ToUint32(),
	}, nil
}
