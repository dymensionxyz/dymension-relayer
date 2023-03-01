package relayer

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/avast/retry-go/v4"
	chantypes "github.com/cosmos/ibc-go/v3/modules/core/04-channel/types"
	ibcexported "github.com/cosmos/ibc-go/v3/modules/core/exported"
	"github.com/cosmos/relayer/v2/relayer/provider"
	"github.com/cosmos/relayer/v2/relayer/provider/cosmos"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// UnrelayedSequences returns the unrelayed sequence numbers between two chains
func unrelayedSequences(ctx context.Context,
	src *Chain, srcChannelId, srcPortId string, srch int64,
	dst *Chain, dstChannelId, dstPortId string, ordering chantypes.Order,
) []uint64 {

	var (
		srcPacketSeq         = []uint64{}
		srcUnreceivedPackets = []uint64{}
		commitments          []*chantypes.PacketState
		err                  error
	)
	if err = retry.Do(func() error {
		// Query the packet commitment
		commitments, err = src.ChainProvider.QueryPacketCommitments(ctx, uint64(srch), srcChannelId, srcPortId)
		switch {
		case err != nil:
			return err
		case len(commitments) == 0:
			return fmt.Errorf("no error on QueryPacketCommitments for %s, however response is nil", src.ChainID())
		default:
			return nil
		}
	}, retry.Context(ctx), RtyAtt, RtyDel, RtyErr, retry.OnRetry(func(n uint, err error) {
		src.log.Info(
			"Failed to query packet commitments",
			zap.String("channel_id", srcChannelId),
			zap.String("port_id", srcPortId),
			zap.Uint("attempt", n+1),
			zap.Uint("max_attempts", RtyAttNum),
			zap.Error(err),
		)
	})); err != nil {
		src.log.Error(
			"Failed to query packet commitments after max retries",
			zap.String("channel_id", srcChannelId),
			zap.String("port_id", srcPortId),
			zap.Uint("attempts", RtyAttNum),
			zap.Error(err),
		)
		return srcUnreceivedPackets
	}

	for _, pc := range commitments {
		srcPacketSeq = append(srcPacketSeq, pc.Sequence)
	}

	if len(srcPacketSeq) > 0 {
		// Query all packets sent by src that have not been received by dst.
		if err := retry.Do(func() error {
			var err error
			// we are using height 0 because we want to check vs the latest height
			srcUnreceivedPackets, err = dst.ChainProvider.QueryUnreceivedPackets(ctx, 0, dstChannelId, dstPortId, srcPacketSeq)
			return err
		}, retry.Context(ctx), RtyAtt, RtyDel, RtyErr, retry.OnRetry(func(n uint, err error) {
			dst.log.Info(
				"Failed to query unreceived packets",
				zap.String("channel_id", dstChannelId),
				zap.String("port_id", dstPortId),
				zap.Uint("attempt", n+1),
				zap.Uint("max_attempts", RtyAttNum),
				zap.Error(err),
			)
		})); err != nil {
			dst.log.Error(
				"Failed to query unreceived packets after max retries",
				zap.String("channel_id", dstChannelId),
				zap.String("port_id", dstPortId),
				zap.Uint("attempts", RtyAttNum),
				zap.Error(err),
			)
			return srcUnreceivedPackets
		}
	}

	// If this is an UNORDERED channel we can return at this point.
	if ordering != chantypes.ORDERED {
		return srcUnreceivedPackets
	}

	// For ordered channels we want to only relay the packet whose sequence number is equal to
	// the expected next packet receive sequence from the counterparty.
	if len(srcUnreceivedPackets) > 0 {
		var srcUnreceivedPacketsOrdered = []uint64{}
		// we are using height 0 because we want to check vs the latest height
		nextSeqResp, err := dst.ChainProvider.QueryNextSeqRecv(ctx, 0, dstChannelId, dstPortId)
		if err != nil {
			dst.log.Error(
				"Failed to query next packet receive sequence",
				zap.String("channel_id", dstChannelId),
				zap.String("port_id", dstPortId),
				zap.Error(err),
			)
			return srcUnreceivedPacketsOrdered
		}

		for _, seq := range srcUnreceivedPackets {
			if seq == nextSeqResp.NextSequenceReceive {
				srcUnreceivedPacketsOrdered = append(srcUnreceivedPacketsOrdered, seq)
				break
			}
		}
		srcUnreceivedPackets = srcUnreceivedPacketsOrdered
	}

	return srcUnreceivedPackets
}

// UnrelayedSequences returns the unrelayed sequence numbers between two chains
func UnrelayedSequences(ctx context.Context, src, dst *Chain, srch, dsth int64, srcChannel *chantypes.IdentifiedChannel) RelaySequences {
	var (
		rs = RelaySequences{Src: []uint64{}, Dst: []uint64{}}
	)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		rs.Src = unrelayedSequences(ctx,
			src, srcChannel.ChannelId, srcChannel.PortId, srch,
			dst, srcChannel.Counterparty.ChannelId, srcChannel.Counterparty.PortId, srcChannel.Ordering)
	}()
	go func() {
		defer wg.Done()
		rs.Dst = unrelayedSequences(ctx,
			dst, srcChannel.Counterparty.ChannelId, srcChannel.Counterparty.PortId, dsth,
			src, srcChannel.ChannelId, srcChannel.PortId, srcChannel.Ordering)
	}()
	wg.Wait()

	return rs
}

// UnrelayedAcknowledgements returns the unrelayed sequence numbers between two chains
func unrelayedAcknowledgements(ctx context.Context,
	src *Chain, srcChannelId, srcPortId string, srch int64,
	dst *Chain, dstChannelId, dstPortId string, dsth int64,
) ([]uint64, error) {
	var (
		srcPacketSeq = []uint64{}
		rs           = []uint64{}
		res          []*chantypes.PacketState
		err          error
	)

	if err = retry.Do(func() error {
		res, err = src.ChainProvider.QueryPacketAcknowledgements(ctx, uint64(srch), srcChannelId, srcPortId)
		switch {
		case err != nil:
			return err
		default:
			return nil
		}
	}, retry.Context(ctx), RtyAtt, RtyDel, RtyErr); err != nil {
		src.log.Error(
			"Failed to query packet acknowledgement commitments after max attempts",
			zap.String("channel_id", srcChannelId),
			zap.String("port_id", srcPortId),
			zap.Uint("attempts", RtyAttNum),
			zap.Error(err),
		)
	}
	if res == nil || err != nil {
		return rs, err
	}
	for _, pc := range res {
		srcPacketSeq = append(srcPacketSeq, pc.Sequence)
	}

	sort.Slice(srcPacketSeq, func(i, j int) bool { return srcPacketSeq[i] < srcPacketSeq[j] })

	if len(srcPacketSeq) > 0 {
		// Query all packets sent by dst that have been received by src
		if err = retry.Do(func() error {
			// we check unreceived vs the latest height
			rs, err = dst.ChainProvider.QueryUnreceivedAcknowledgements(ctx, 0, dstChannelId, dstPortId, srcPacketSeq)
			return err
		}, retry.Context(ctx), RtyAtt, RtyDel, RtyErr); err != nil {
			dst.log.Error(
				"Failed to query unreceived acknowledgements after max attempts",
				zap.String("channel_id", dstChannelId),
				zap.String("port_id", dstPortId),
				zap.Uint("attempts", RtyAttNum),
				zap.Error(err),
			)
		}
	}

	return rs, err
}

// UnrelayedAcknowledgements returns the unrelayed sequence numbers between two chains
func UnrelayedAcknowledgements(ctx context.Context, src, dst *Chain, srch, dsth int64, srcChannel *chantypes.IdentifiedChannel) RelaySequences {
	var (
		rs = RelaySequences{Src: []uint64{}, Dst: []uint64{}}
	)
	var (
		errSrc, errDst error
	)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		rs.Src, errSrc = unrelayedAcknowledgements(ctx,
			src, srcChannel.ChannelId, srcChannel.PortId, srch,
			dst, srcChannel.Counterparty.ChannelId, srcChannel.Counterparty.PortId, dsth)
	}()
	go func() {
		defer wg.Done()
		rs.Dst, errDst = unrelayedAcknowledgements(ctx,
			dst, srcChannel.Counterparty.ChannelId, srcChannel.Counterparty.PortId, dsth,
			src, srcChannel.ChannelId, srcChannel.PortId, srch)
	}()
	wg.Wait()

	_, _ = errSrc, errDst
	return rs
}

// RelaySequences represents unrelayed packets on src and dst
type RelaySequences struct {
	Src []uint64 `json:"src"`
	Dst []uint64 `json:"dst"`
}

func (rs *RelaySequences) Empty() bool {
	if len(rs.Src) == 0 && len(rs.Dst) == 0 {
		return true
	}
	return false
}

// relayAcknowledgements creates transactions to relay acknowledgements from src
// to dst following the sequences of the packets that were acked on src
func relayAcknowledgements(ctx context.Context, log *zap.Logger,
	src *Chain, srcChannelId, srcPortId string, srch int64, sequences []uint64,
	dst *Chain, dstChannelId, dstPortId string,
	maxTxSize, maxMsgLength uint64, memo string,
) error {
	// set the maximum relay transaction constraints
	msgs := []provider.RelayerMessage{}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// add messages for received packets on src
		for _, seq := range sequences {
			// src wrote the ack. acknowledgementFromSequence will query the acknowledgement
			// from the counterparty chain (second chain provided in the arguments). The message
			// should be sent to dst.
			//elayAckMsgs, err :=dst.ChainProvider.AcknowledgementFromSequence(ctx, src.ChainProvider, uint64(srch), seq, dstChannel.Counterparty.ChannelId, dstChannel.Counterparty.PortId, dstChannel.ChannelId, dstChannel.PortId)
			relayAckMsgs, err := dst.ChainProvider.AcknowledgementFromSequence(
				ctx,
				src.ChainProvider, uint64(srch), seq, srcChannelId, srcPortId,
				dstChannelId, dstPortId)
			if err != nil {
				return err
			}

			// Do not allow nil messages to the queued, or else we will panic in send()
			if relayAckMsgs != nil {
				msgs = append(msgs, relayAckMsgs)
			}
		}

		if len(msgs) == 0 {
			log.Info(
				"No acknowledgements to relay",
				zap.String("src_chain_id", src.ChainID()),
				zap.String("src_port_id", srcPortId),
				zap.String("dst_chain_id", dst.ChainID()),
				zap.String("dst_port_id", dstPortId),
			)
			return nil
		}

		err := PrependUpdateClientMsg(ctx, &msgs, src, dst, srch)

		if err != nil {
			return err
		}

		// send messages to their respective chains
		var successfulBatches int
		Send(ctx, log, AsRelayMsgSender(dst), msgs, memo, &successfulBatches, &err, maxMsgLength, maxTxSize)

		if (successfulBatches > 0) && (err != nil) {
			log.Info(
				"Partial success when relaying acknowledgements",
				zap.String("src_chain_id", src.ChainID()),
				zap.String("src_port_id", srcPortId),
				zap.String("dst_chain_id", dst.ChainID()),
				zap.String("dst_port_id", dstPortId),
				zap.Error(err),
			)
			return err
		}

		if successfulBatches > 0 {
			dst.logPacketsRelayed(src, successfulBatches, dstPortId, srcPortId)
		}
	}

	return nil
}

// RelayAcknowledgements creates transactions to relay acknowledgements from src to dst and from dst to src
func RelayAcknowledgements(ctx context.Context, log *zap.Logger, src, dst *Chain, srch, dsth int64, sp RelaySequences, maxTxSize, maxMsgLength uint64, memo string, srcChannel *chantypes.IdentifiedChannel) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		var wg sync.WaitGroup
		var srcErr, DstErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			srcErr = relayAcknowledgements(ctx, log,
				src, srcChannel.ChannelId, srcChannel.PortId, srch, sp.Src,
				dst, srcChannel.Counterparty.ChannelId, srcChannel.Counterparty.PortId,
				maxTxSize, maxMsgLength, memo)
		}()
		go func() {
			defer wg.Done()
			DstErr = relayAcknowledgements(ctx, log,
				dst, srcChannel.Counterparty.ChannelId, srcChannel.Counterparty.PortId, dsth, sp.Dst,
				src, srcChannel.ChannelId, srcChannel.PortId,
				maxTxSize, maxMsgLength, memo)
		}()
		wg.Wait()

		if srcErr != nil {
			println(srcErr.Error())
		}
		if DstErr != nil {
			println(srcErr.Error())
		}
	}
	return nil
}

// RelayPackets creates transactions to relay packets from src to dst and from dst to src
func RelayPackets(ctx context.Context, log *zap.Logger, src, dst *Chain, srch, dsth int64, sp RelaySequences, maxTxSize, maxMsgLength uint64, memo string, srcChannel *chantypes.IdentifiedChannel) error {
	// set the maximum relay transaction constraints
	msgs := &RelayMsgs{
		Src:          []provider.RelayerMessage{},
		Dst:          []provider.RelayerMessage{},
		MaxTxSize:    maxTxSize,
		MaxMsgLength: maxMsgLength,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:

		eg, egCtx := errgroup.WithContext(ctx)
		// add messages for sequences on src
		eg.Go(func() error {
			return AddMessagesForSequences(ctx, sp.Src, src, dst, srch, dsth, &msgs.Src, &msgs.Dst,
				srcChannel.ChannelId, srcChannel.PortId, srcChannel.Counterparty.ChannelId, srcChannel.Counterparty.PortId, srcChannel.Ordering)
		})

		// add messages for sequences on dst
		eg.Go(func() error {
			return AddMessagesForSequences(ctx, sp.Dst, dst, src, dsth, srch, &msgs.Dst, &msgs.Src,
				srcChannel.Counterparty.ChannelId, srcChannel.Counterparty.PortId, srcChannel.ChannelId, srcChannel.PortId, srcChannel.Ordering)
		})

		if err := eg.Wait(); err != nil {
			return err
		}

		if !msgs.Ready() {
			log.Info(
				"No packets to relay",
				zap.String("src_chain_id", src.ChainID()),
				zap.String("src_port_id", srcChannel.PortId),
				zap.String("dst_chain_id", dst.ChainID()),
				zap.String("dst_port_id", srcChannel.Counterparty.PortId),
			)
			return nil
		}

		// Prepend non-empty msg lists with UpdateClient

		eg, egCtx = errgroup.WithContext(ctx) // New errgroup because previous egCtx is canceled at this point.
		eg.Go(func() error {
			return PrependUpdateClientMsg(egCtx, &msgs.Dst, src, dst, srch)
		})

		eg.Go(func() error {
			return PrependUpdateClientMsg(egCtx, &msgs.Src, dst, src, dsth)
		})

		if err := eg.Wait(); err != nil {
			return err
		}

		// send messages to their respective chains
		result := msgs.Send(ctx, log, AsRelayMsgSender(src), AsRelayMsgSender(dst), memo)
		if err := result.Error(); err != nil {
			if result.PartiallySent() {
				log.Info(
					"Partial success when relaying packets",
					zap.String("src_chain_id", src.ChainID()),
					zap.String("src_port_id", srcChannel.PortId),
					zap.String("dst_chain_id", dst.ChainID()),
					zap.String("dst_port_id", srcChannel.Counterparty.PortId),
					zap.Error(err),
				)
			}
			return err
		}

		if result.SuccessfulSrcBatches > 0 {
			src.logPacketsRelayed(dst, result.SuccessfulSrcBatches, srcChannel.PortId, srcChannel.Counterparty.PortId)
		}
		if result.SuccessfulDstBatches > 0 {
			dst.logPacketsRelayed(src, result.SuccessfulDstBatches, srcChannel.PortId, srcChannel.Counterparty.PortId)
		}

		return nil
	}
}

// AddMessagesForSequences constructs RecvMsgs and TimeoutMsgs from sequence numbers on a src chain
// and adds them to the appropriate queue of msgs for both src and dst
func AddMessagesForSequences(
	ctx context.Context,
	sequences []uint64,
	src, dst *Chain,
	srch, dsth int64,
	srcMsgs, dstMsgs *[]provider.RelayerMessage,
	srcChanID, srcPortID, dstChanID, dstPortID string,
	order chantypes.Order,
) error {
	for _, seq := range sequences {
		recvMsg, timeoutMsg, err := src.ChainProvider.RelayPacketFromSequence(
			ctx,
			src.ChainProvider, dst.ChainProvider,
			uint64(srch), uint64(dsth),
			seq,
			dstChanID, dstPortID, dst.ClientID(),
			srcChanID, srcPortID, src.ClientID(),
			order,
		)
		if err != nil {
			src.log.Info(
				"Failed to relay packet from sequence",
				zap.String("src_chain_id", src.ChainID()),
				zap.String("src_channel_id", srcChanID),
				zap.String("src_port_id", srcPortID),
				zap.String("dst_chain_id", dst.ChainID()),
				zap.String("dst_channel_id", dstChanID),
				zap.String("dst_port_id", dstPortID),
				zap.String("channel_order", order.String()),
				zap.Error(err),
			)
			return err
		}

		// Depending on the type of message to be relayed, we need to send to different chains
		if recvMsg != nil {
			*dstMsgs = append(*dstMsgs, recvMsg)
		}

		if timeoutMsg != nil {
			*srcMsgs = append(*srcMsgs, timeoutMsg)
		}
	}

	return nil
}

// PrependUpdateClientMsg adds an UpdateClient msg to the front of non-empty msg lists
func PrependUpdateClientMsg(ctx context.Context, msgs *[]provider.RelayerMessage, src, dst *Chain, srch int64) error {
	if len(*msgs) == 0 {
		return nil
	}

	// Query IBC Update Header
	var srcHeader ibcexported.Header
	if err := retry.Do(func() error {
		var err error
		srcHeader, err = src.ChainProvider.GetIBCUpdateHeader(ctx, srch, dst.ChainProvider, dst.PathEnd.ClientID)
		return err
	}, retry.Context(ctx), RtyAtt, RtyDel, RtyErr, retry.OnRetry(func(n uint, err error) {
		src.log.Info(
			"PrependUpdateClientMsg: failed to get IBC update header",
			zap.String("src_chain_id", src.ChainID()),
			zap.String("dst_chain_id", dst.ChainID()),
			zap.Uint("attempt", n+1),
			zap.Uint("attempt_limit", RtyAttNum),
			zap.Error(err),
		)

	})); err != nil {
		return err
	}

	// Construct UpdateClient msg
	var updateMsg provider.RelayerMessage
	if err := retry.Do(func() error {
		var err error
		updateMsg, err = dst.ChainProvider.MsgUpdateClient(dst.PathEnd.ClientID, srcHeader)
		return err
	}, retry.Context(ctx), RtyAtt, RtyDel, RtyErr, retry.OnRetry(func(n uint, err error) {
		dst.log.Info(
			"PrependUpdateClientMsg: failed to build message",
			zap.String("dst_chain_id", dst.ChainID()),
			zap.Uint("attempt", n+1),
			zap.Uint("attempt_limit", RtyAttNum),
			zap.Error(err),
		)
	})); err != nil {
		return err
	}

	trustedHeight, err := cosmos.GetTrustedHeight(srcHeader)
	if err != nil {
		return err
	}
	// no need to append update message, the client is already updated
	if srcHeader.GetHeight().LTE(*trustedHeight) {
		return nil
	}
	// Prepend UpdateClient msg to the slice of msgs
	*msgs = append([]provider.RelayerMessage{updateMsg}, *msgs...)

	return nil
}
