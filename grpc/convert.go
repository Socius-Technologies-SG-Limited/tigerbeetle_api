package grpc

import (
	"log/slog"

	"github.com/lil5/tigerbeetle_api/proto"
	"github.com/samber/lo"
	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
)

func AccountToProtoAccount(tbAccount tigerbeetle_go.Account) *proto.Account {
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
		DebitsPending:  tbAccount.DebitsPending.BigInt().Uint64(),
		DebitsPosted:   tbAccount.DebitsPosted.BigInt().Uint64(),
		CreditsPending: tbAccount.CreditsPending.BigInt().Uint64(),
		CreditsPosted:  tbAccount.CreditsPosted.BigInt().Uint64(),
		UserData128:    tbAccount.UserData128.String(),
		UserData64:     tbAccount.UserData64,
		UserData32:     tbAccount.UserData32,
		Ledger:         tbAccount.Ledger,
		Code:           uint32(tbAccount.Code),
		Flags:          &pFlags,
		Timestamp:      uint64(tbAccount.Timestamp),
	}
}

func TransferToProtoTransfer(tbTransfer tigerbeetle_go.Transfer) *proto.Transfer {
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
	emptyUint128 := tigerbeetle_go.Uint128{}
	if tbTransfer.PendingID != emptyUint128 {
		pendingId = tbTransfer.PendingID.String()
	}
	return &proto.Transfer{
		Id:              tbTransfer.ID.String(),
		DebitAccountId:  tbTransfer.DebitAccountID.String(),
		CreditAccountId: tbTransfer.CreditAccountID.String(),
		Amount:          tbTransfer.Amount.BigInt().Int64(),
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

func AccountFilterFromProtoToTigerbeetle(pAccountFilter *proto.AccountFilter) (*tigerbeetle_go.AccountFilter, error) {
	accountID, err := HexStringToUint128(pAccountFilter.AccountId)
	if err != nil {
		return nil, err
	}

	var userData128 tigerbeetle_go.Uint128
	if pAccountFilter.UserData128 != nil && *pAccountFilter.UserData128 != "" {
		userData128, err = tigerbeetle_go.HexStringToUint128(*pAccountFilter.UserData128)
		if err != nil {
			slog.Error("invalid UserData128 hex string", "hex", *pAccountFilter.UserData128, "error", err)
			return nil, err
		}
	}

	var tbFlags tigerbeetle_go.AccountFilterFlags
	if pAccountFilter.Flags != nil {
		tbFlags = tigerbeetle_go.AccountFilterFlags{
			Debits:   lo.FromPtrOr(pAccountFilter.Flags.Debits, false),
			Credits:  lo.FromPtrOr(pAccountFilter.Flags.Credits, false),
			Reversed: lo.FromPtrOr(pAccountFilter.Flags.Reversed, false),
		}
	}

	return &tigerbeetle_go.AccountFilter{
		AccountID:    *accountID,
		UserData128:  userData128,
		UserData64:   lo.FromPtrOr(pAccountFilter.UserData64, 0),
		UserData32:   lo.FromPtrOr(pAccountFilter.UserData32, 0),
		Code:         uint16(lo.FromPtrOr(pAccountFilter.Code, 0)),
		TimestampMin: uint64(lo.FromPtrOr(pAccountFilter.TimestampMin, 0)),
		TimestampMax: uint64(lo.FromPtrOr(pAccountFilter.TimestampMax, 0)),
		Limit:        uint32(pAccountFilter.Limit),
		Flags:        tbFlags.ToUint32(),
	}, nil
}

func AccountBalanceFromTigerbeetleToProto(tbBalance tigerbeetle_go.AccountBalance) *proto.AccountBalance {
	return &proto.AccountBalance{
		DebitsPending:  tbBalance.DebitsPending.BigInt().Uint64(),
		DebitsPosted:   tbBalance.DebitsPosted.BigInt().Uint64(),
		CreditsPending: tbBalance.CreditsPending.BigInt().Uint64(),
		CreditsPosted:  tbBalance.CreditsPosted.BigInt().Uint64(),
		Timestamp:      tbBalance.Timestamp,
	}
}

func HexStringToUint128(hex string) (*tigerbeetle_go.Uint128, error) {
	if hex == "" {
		return &tigerbeetle_go.Uint128{0}, nil
	}

	res, err := tigerbeetle_go.HexStringToUint128(hex)
	if err != nil {
		slog.Error("hex string to Uint128 failed", "hex", hex, "error", err)
		return nil, err
	}
	return &res, nil

}

// AccountResultsToReply converts a 0.17 dense CreateAccounts result slice
// into proto reply items. Result i corresponds positionally to input account
// i, so the caller must invoke us with the same accounts slice it submitted.
func AccountResultsToReply(results []tigerbeetle_go.CreateAccountResult, accounts []tigerbeetle_go.Account) []*proto.CreateAccountsReplyItem {
	replies := make([]*proto.CreateAccountsReplyItem, 0, len(results))
	for i, r := range results {
		_ = accounts[i] // positional invariant: len(results) == len(accounts) in 0.17
		replies = append(replies, &proto.CreateAccountsReplyItem{
			Status:    mapAccountStatus(r.Status),
			Timestamp: r.Timestamp,
		})
	}
	return replies
}

// ResultsToReply converts a 0.17 dense CreateTransfers result slice into
// proto reply items. Same positional invariant as AccountResultsToReply.
func ResultsToReply(results []tigerbeetle_go.CreateTransferResult, transfers []tigerbeetle_go.Transfer) []*proto.CreateTransfersReplyItem {
	replies := make([]*proto.CreateTransfersReplyItem, 0, len(results))
	for i, r := range results {
		replies = append(replies, &proto.CreateTransfersReplyItem{
			Status:    mapTransferStatus(r.Status),
			Id:        transfers[i].ID.String(),
			Timestamp: r.Timestamp,
		})
	}
	return replies
}

// mapAccountStatus / mapTransferStatus translate between the native SDK's
// 0xFFFFFFFF success sentinel and the proto-idiomatic 0. See proto/tigerbeetle.proto
// for the rationale.
func mapAccountStatus(s tigerbeetle_go.CreateAccountStatus) proto.CreateAccountStatus {
	if s == tigerbeetle_go.AccountCreated {
		return proto.CreateAccountStatus_AccountCreated
	}
	return proto.CreateAccountStatus(s)
}

func mapTransferStatus(s tigerbeetle_go.CreateTransferStatus) proto.CreateTransferStatus {
	if s == tigerbeetle_go.TransferCreated {
		return proto.CreateTransferStatus_TransferCreated
	}
	return proto.CreateTransferStatus(s)
}

func QueryFilterFromProtoToTigerbeetle(pFilter *proto.QueryFilter) (*tigerbeetle_go.QueryFilter, error) {
	if pFilter == nil {
		return nil, nil
	}

	var userData128 tigerbeetle_go.Uint128
	if pFilter.UserData128 != nil && *pFilter.UserData128 != "" {
		var err error
		userData128, err = tigerbeetle_go.HexStringToUint128(*pFilter.UserData128)
		if err != nil {
			slog.Error("invalid UserData128 hex string", "hex", *pFilter.UserData128, "error", err)
			return nil, err
		}
	}

	var flags tigerbeetle_go.QueryFilterFlags
	if pFilter.Flags != nil {
		flags = tigerbeetle_go.QueryFilterFlags{
			Reversed: lo.FromPtrOr(pFilter.Flags.Reversed, false),
		}
	}

	return &tigerbeetle_go.QueryFilter{
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
