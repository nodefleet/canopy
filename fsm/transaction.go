package fsm

import (
	"github.com/canopy-network/canopy/lib"
	"github.com/canopy-network/canopy/lib/crypto"
	"google.golang.org/protobuf/types/known/anypb"
	"time"
)

/* This file contains transaction handling logic - for the payload handling check message.go */

// define some safe +/- tx indexer prune height
const BlockAcceptanceRange = 4320

// ApplyTransaction() processes the transaction within the state machine, returning the corresponding TxResult.
func (s *StateMachine) ApplyTransaction(index uint64, transaction []byte, txHash string, batchVerifier *crypto.BatchVerifier) (*lib.TxResult, lib.ErrorI) {
	// validate the transaction and get the check result
	result, err := s.CheckTx(transaction, txHash, batchVerifier)
	if err != nil {
		return nil, err
	}
	// if the transaction is meant for the plugin
	if result.plugin {
		// route to plugin
		resp, e := s.Plugin.DeliverTx(s, &lib.PluginDeliverRequest{Tx: result.tx})
		// handle error
		if err = e; err != nil {
			return nil, err
		}
		// if the response contains an error
		if err = resp.Error.E(); err != nil {
			return nil, err
		}
	} else {
		// deduct fees for the transaction
		if err = s.AccountDeductFees(result.sender, result.tx.Fee); err != nil {
			return nil, err
		}
		// handle the message (payload)
		if err = s.HandleMessage(result.msg); err != nil {
			return nil, err
		}
	}
	// return the tx result
	return &lib.TxResult{
		Sender:      result.sender.Bytes(),
		Recipient:   result.recipient,
		MessageType: result.tx.MessageType,
		Height:      s.Height(),
		Index:       index,
		Transaction: result.tx,
		TxHash:      txHash,
	}, nil
}

// CheckTx() validates the transaction object
func (s *StateMachine) CheckTx(transaction []byte, txHash string, batchVerifier *crypto.BatchVerifier) (result *CheckTxResult, err lib.ErrorI) {
	// create various result variables
	var (
		authorizedSigners [][]byte
		msg               lib.MessageI
		recipient         []byte
		plugin            bool
	)
	tx := new(lib.Transaction)
	// populate the object ref with the bytes of the transaction
	if err = lib.Unmarshal(transaction, tx); err != nil {
		return
	}
	// perform basic validations against the tx object
	if err = tx.CheckBasic(); err != nil {
		return
	}
	// validate the timestamp (prune friendly - replay protection)
	if err = s.CheckReplay(tx, txHash); err != nil {
		return
	}
	// if the transaction is meant for the plugin
	if s.Plugin.SupportsTransaction(tx.MessageType) {
		// execute check tx on the plugin
		resp, e := s.Plugin.CheckTx(s, &lib.PluginCheckRequest{Tx: tx})
		if err = e; err != nil {
			return
		}
		// check if response errored
		if err = resp.Error.E(); err != nil {
			return
		}
		// set various result variables
		authorizedSigners, recipient, plugin = resp.AuthorizedSigners, resp.Recipient, true
	} else {
		// perform basic validations against the message payload
		msg, err = s.CheckMessage(tx.Msg)
		if err != nil {
			return
		}
		// validate the fee associated with the transaction
		if err = s.CheckFee(tx.Fee, msg); err != nil {
			return
		}
		// check the authorized signers for the message
		authorizedSigners, err = s.GetAuthorizedSignersFor(msg)
		if err != nil {
			return
		}
		// set recipient
		recipient = msg.Recipient()
	}
	// validate the signature of the transaction
	sender, err := s.CheckSignature(tx, authorizedSigners, batchVerifier)
	if err != nil {
		return
	}
	// populate special message fields (if applicable)
	s.PopulateSpecialMessageFields(tx, sender, msg)
	// return the result
	return &CheckTxResult{
		tx:        tx,
		msg:       msg,
		sender:    sender,
		recipient: recipient,
		plugin:    plugin,
	}, nil
}

// CheckTxResult is the result object from CheckTx()
type CheckTxResult struct {
	tx        *lib.Transaction // the transaction object
	msg       lib.MessageI     // the payload message in the transaction
	sender    crypto.AddressI  // the sender address of the transaction
	recipient []byte           // the recipient of the transaction (if applicable)
	plugin    bool             // if the transaction is handled by the plugin
}

// CheckSignature() validates the signer and the digital signature associated with the transaction object
func (s *StateMachine) CheckSignature(tx *lib.Transaction, authorizedSigners [][]byte, batchSigVerifier *crypto.BatchVerifier) (crypto.AddressI, lib.ErrorI) {
	// validate the actual signature bytes
	if tx.Signature == nil || len(tx.Signature.Signature) == 0 {
		return nil, ErrEmptySignature()
	}
	// get the canonical byte representation of the transaction
	signBytes, err := tx.GetSignBytes()
	if err != nil {
		return nil, ErrTxSignBytes(err)
	}
	// convert signature bytes to public key object
	publicKey, e := crypto.NewPublicKeyFromBytes(tx.Signature.PublicKey)
	if e != nil {
		return nil, ErrInvalidPublicKey(e)
	}
	// special case: check for a special RLP transaction
	if _, hasEthPubKey := publicKey.(*crypto.ETHSECP256K1PublicKey); hasEthPubKey && tx.Memo == RLPIndicator {
		if err = s.VerifyRLPBytes(tx); err != nil {
			return nil, err
		}
	} else {
		// if using a batch verifier
		if batchSigVerifier != nil {
			if e = batchSigVerifier.Add(publicKey, tx.Signature.PublicKey, signBytes, tx.Signature.Signature); e != nil {
				return nil, ErrInvalidPublicKey(e)
			}
		} else {
			// if verifying 1 by 1
			if !publicKey.VerifyBytes(signBytes, tx.Signature.Signature) {
				return nil, ErrInvalidSignature()
			}
		}
	}
	// calculate the corresponding address from the public key
	address := publicKey.Address()
	// for each authorized signer
	for _, authorized := range authorizedSigners {
		// if the address that signed the transaction matches one of the authorized signers
		if address.Equals(crypto.NewAddressFromBytes(authorized)) {
			// return the signer address
			return address, nil
		}
	}
	// if no authorized signer matched the signer address, it's unauthorized
	return nil, ErrUnauthorizedTx()
}

// CheckReplay() validates the timestamp of the transaction
// Instead of using an increasing 'sequence number' Canopy uses timestamp + created block to act as a prune-friendly, replay attack / hash collision prevention mechanism
//   - Canopy searches the transaction indexer for the transaction using its hash to prevent 'replay attacks'
//   - The timestamp protects against hash collisions as it injects 'micro-second level entropy'
//     into the hash of the transaction, ensuring no transactions will 'accidentally collide'
//   - The created block acceptance policy for transactions maintains an acceptable bound of 'time' to support database pruning
func (s *StateMachine) CheckReplay(tx *lib.Transaction, txHash string) lib.ErrorI {
	// ensure the right network
	if uint64(s.NetworkID) != tx.NetworkId {
		return lib.ErrWrongNetworkID()
	}
	// ensure the right chain
	if s.Config.ChainId != tx.ChainId {
		return lib.ErrWrongChainId()
	}
	// if below height 2, skip this check as GetBlockByHeight will load a block that has a lastQC that doesn't exist
	if s.Height() < 2 {
		return nil
	}
	// if checking the transaction hash
	if txHash != "" {
		// ensure the store can 'read the indexer'
		store, ok := s.store.(lib.RIndexerI)
		// if it can't then exit
		if !ok {
			return ErrWrongStoreType()
		}
		// convert the transaction hash string into bytes
		hashBz, err := lib.StringToBytes(txHash)
		if err != nil {
			return err
		}
		// ensure the tx doesn't already exist in the indexer
		// same block replays are protected at a higher level
		txResult, err := store.GetTxByHash(hashBz)
		if err != nil {
			return err
		}
		// if the tx transaction result isn't nil, and it has a hash
		if txResult != nil && txResult.TxHash == txHash {
			return lib.ErrDuplicateTx(txHash)
		}
	}
	// this gives the protocol a theoretically safe tx indexer prune height
	maxHeight, minHeight := s.Height()+BlockAcceptanceRange, uint64(0)
	// if height is after the BlockAcceptanceRange blocks
	if s.Height() > BlockAcceptanceRange {
		// update the minimum height
		minHeight = s.Height() - BlockAcceptanceRange
	}
	// ensure the tx 'created height' is not above or below the acceptable bounds
	if tx.CreatedHeight > maxHeight || tx.CreatedHeight < minHeight {
		return lib.ErrInvalidTxHeight()
	}
	// exit
	return nil
}

// CheckMessage() performs basic validations on the msg payload
func (s *StateMachine) CheckMessage(msg *anypb.Any) (message lib.MessageI, err lib.ErrorI) {
	// ensure the message isn't nil
	if msg == nil {
		return nil, lib.ErrEmptyMessage()
	}
	// extract the message from an protobuf any
	proto, err := lib.FromAny(msg)
	if err != nil {
		return nil, err
	}
	// cast the proto message to a Message interface that may be interpreted
	message, ok := proto.(lib.MessageI)
	// if cast fails, throw an error
	if !ok {
		return nil, ErrInvalidTxMessage()
	}
	// do stateless checks on the message
	if err = message.Check(); err != nil {
		return nil, err
	}
	// return the message as the interface
	return message, nil
}

// CheckFee() validates the fee amount is sufficient to pay for a transaction
func (s *StateMachine) CheckFee(fee uint64, msg lib.MessageI) (err lib.ErrorI) {
	// get the fee for the message name
	stateLimitFee, err := s.GetFeeForMessageName(msg.Name())
	if err != nil {
		return err
	}
	// if the fee is below the limit
	if fee < stateLimitFee {
		return ErrTxFeeBelowStateLimit()
	}
	// exit
	return
}

// HandleSpecialMessageFields() populates special message fields based on the message type
func (s *StateMachine) PopulateSpecialMessageFields(tx lib.TransactionI, signer crypto.AddressI, msg lib.MessageI) {
	// if message isn't nil
	if msg != nil {
		// handle special fields for transactions
		switch x := msg.(type) {
		case *MessageStake:
			// populate the signer field for stake
			x.Signer = signer.Bytes()
		case *MessageEditStake:
			// populate the signer field for edit-stake
			x.Signer = signer.Bytes()
		case *MessageChangeParameter:
			// populate the proposal hash for change parameter
			hash, _ := tx.GetHash()
			x.ProposalHash = lib.BytesToString(hash)
		case *MessageDAOTransfer:
			// populate the proposal hash for dao transfer
			hash, _ := tx.GetHash()
			x.ProposalHash = lib.BytesToString(hash)
		case *MessageCreateOrder:
			// populate the order id for the create order
			hash, _ := tx.GetHash()
			x.OrderId = hash[:20] // first 20 bytes of the transaction hash
		}
	}
}

// NewSendTransaction() creates a SendTransaction object in the interface form of TransactionI
func NewSendTransaction(from crypto.PrivateKeyI, to crypto.AddressI, amount, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(from, &MessageSend{
		FromAddress: from.PublicKey().Address().Bytes(),
		ToAddress:   to.Bytes(),
		Amount:      amount,
	}, networkId, chainId, fee, height, memo)
}

// NewStakeTx() creates a StakeTransaction object in the interface form of TransactionI
func NewStakeTx(signer crypto.PrivateKeyI, from lib.HexBytes, outputAddress crypto.AddressI, netAddress string, committees []uint64, amount, networkId, chainId, fee, height uint64, delegate, earlyWithdrawal bool, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(signer, &MessageStake{
		PublicKey:     from,
		Amount:        amount,
		Committees:    committees,
		NetAddress:    netAddress,
		OutputAddress: outputAddress.Bytes(),
		Delegate:      delegate,
		Compound:      !earlyWithdrawal,
	}, networkId, chainId, fee, height, memo)
}

// NewEditStakeTx() creates a EditStakeTransaction object in the interface form of TransactionI
func NewEditStakeTx(signer crypto.PrivateKeyI, from, outputAddress crypto.AddressI, netAddress string, committees []uint64, amount, networkId, chainId, fee, height uint64, earlyWithdrawal bool, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(signer, &MessageEditStake{
		Address:       from.Bytes(),
		Amount:        amount,
		Committees:    committees,
		NetAddress:    netAddress,
		OutputAddress: outputAddress.Bytes(),
		Compound:      !earlyWithdrawal,
	}, networkId, chainId, fee, height, memo)
}

// NewUnstakeTx() creates a UnstakeTransaction object in the interface form of TransactionI
func NewUnstakeTx(signer crypto.PrivateKeyI, from crypto.AddressI, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(signer, &MessageUnstake{Address: from.Bytes()}, networkId, chainId, fee, height, memo)
}

// NewPauseTx() creates a PauseTransaction object in the interface form of TransactionI
func NewPauseTx(signer crypto.PrivateKeyI, from crypto.AddressI, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(signer, &MessagePause{Address: from.Bytes()}, networkId, chainId, fee, height, memo)
}

// NewUnpauseTx() creates a UnpauseTransaction object in the interface form of TransactionI
func NewUnpauseTx(signer crypto.PrivateKeyI, from crypto.AddressI, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(signer, &MessageUnpause{Address: from.Bytes()}, networkId, chainId, fee, height, memo)
}

// NewChangeParamTxUint64() creates a ChangeParamTransaction object (for uint64s) in the interface form of TransactionI
func NewChangeParamTxUint64(from crypto.PrivateKeyI, space, key string, value, start, end, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	a, err := lib.NewAny(&lib.UInt64Wrapper{Value: value})
	if err != nil {
		return nil, err
	}
	return NewTransaction(from, &MessageChangeParameter{
		ParameterSpace: space,
		ParameterKey:   key,
		ParameterValue: a,
		StartHeight:    start,
		EndHeight:      end,
		Signer:         from.PublicKey().Address().Bytes(),
	}, networkId, chainId, fee, height, memo)
}

// NewChangeParamTxString() creates a ChangeParamTransaction object (for strings) in the interface form of TransactionI
func NewChangeParamTxString(from crypto.PrivateKeyI, space, key, value string, start, end, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	a, err := lib.NewAny(&lib.StringWrapper{Value: value})
	if err != nil {
		return nil, err
	}
	return NewTransaction(from, &MessageChangeParameter{
		ParameterSpace: space,
		ParameterKey:   key,
		ParameterValue: a,
		StartHeight:    start,
		EndHeight:      end,
		Signer:         from.PublicKey().Address().Bytes(),
	}, networkId, chainId, fee, height, memo)
}

// NewDAOTransferTx() creates a DAOTransferTransaction object in the interface form of TransactionI
func NewDAOTransferTx(from crypto.PrivateKeyI, amount, start, end, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(from, &MessageDAOTransfer{
		Address:     from.PublicKey().Address().Bytes(),
		Amount:      amount,
		StartHeight: start,
		EndHeight:   end,
	}, networkId, chainId, fee, height, memo)
}

// NewCertificateResultsTx() creates a CertificateResultsTransaction object in the interface form of TransactionI
func NewCertificateResultsTx(from crypto.PrivateKeyI, qc *lib.QuorumCertificate, rootChainId, networkId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(from, &MessageCertificateResults{Qc: qc}, networkId, rootChainId, fee, height, memo)
}

// NewSubsidyTx() creates a SubsidyTransaction object in the interface form of TransactionI
func NewSubsidyTx(from crypto.PrivateKeyI, amount, committeeId uint64, opCode lib.HexBytes, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(from, &MessageSubsidy{
		Address: from.PublicKey().Address().Bytes(),
		ChainId: committeeId,
		Amount:  amount,
		Opcode:  opCode,
	}, networkId, chainId, fee, height, memo)
}

// NewCreateOrderTx() creates a CreateOrderTransaction object in the interface form of TransactionI
func NewCreateOrderTx(from crypto.PrivateKeyI, sellAmount, requestAmount, committeeId uint64, data lib.HexBytes, receiveAddress []byte, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	return NewTransaction(from, &MessageCreateOrder{
		ChainId:              committeeId,
		Data:                 data,
		AmountForSale:        sellAmount,
		RequestedAmount:      requestAmount,
		SellerReceiveAddress: receiveAddress,
		SellersSendAddress:   from.PublicKey().Address().Bytes(),
	}, networkId, chainId, fee, height, memo)
}

// NewEditOrderTx() creates an EditOrderTransaction object in the interface form of TransactionI
func NewEditOrderTx(from crypto.PrivateKeyI, orderId string, sellAmount, requestAmount, committeeId uint64, data lib.HexBytes, receiveAddress []byte, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	oId, err := lib.StringToBytes(orderId)
	if err != nil {
		return nil, err
	}
	return NewTransaction(from, &MessageEditOrder{
		OrderId:              oId,
		ChainId:              committeeId,
		Data:                 data,
		AmountForSale:        sellAmount,
		RequestedAmount:      requestAmount,
		SellerReceiveAddress: receiveAddress,
	}, networkId, chainId, fee, height, memo)
}

// NewDeleteOrderTx() creates an DeleteOrderTransaction object in the interface form of TransactionI
func NewDeleteOrderTx(from crypto.PrivateKeyI, orderId string, committeeId uint64, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	oId, err := lib.StringToBytes(orderId)
	if err != nil {
		return nil, err
	}
	return NewTransaction(from, &MessageDeleteOrder{
		OrderId: oId,
		ChainId: committeeId,
	}, networkId, chainId, fee, height, memo)
}

// NewLockOrderTx() reserves a sell order using a send-tx and the memo field
func NewLockOrderTx(from crypto.PrivateKeyI, order lib.LockOrder, networkId, chainId, fee, height uint64) (lib.TransactionI, lib.ErrorI) {
	jsonBytes, err := lib.MarshalJSON(order)
	if err != nil {
		return nil, err
	}
	return NewSendTransaction(from, from.PublicKey().Address(), 1, networkId, chainId, fee, height, string(jsonBytes))
}

// NewCloseOrderTx() completes the purchase of a sell order using a send-tx and the memo field
func NewCloseOrderTx(from crypto.PrivateKeyI, order lib.CloseOrder, buyersReceiveAddress crypto.AddressI, amount, networkId, chainId, fee, height uint64) (lib.TransactionI, lib.ErrorI) {
	jsonBytes, err := lib.MarshalJSON(order)
	if err != nil {
		return nil, err
	}
	return NewSendTransaction(from, buyersReceiveAddress, amount, networkId, chainId, fee, height, string(jsonBytes))
}

// NewTransaction() creates a Transaction object from a message in the interface form of TransactionI
func NewTransaction(pk crypto.PrivateKeyI, msg lib.MessageI, networkId, chainId, fee, height uint64, memo string) (lib.TransactionI, lib.ErrorI) {
	a, err := lib.NewAny(msg)
	if err != nil {
		return nil, err
	}
	tx := &lib.Transaction{
		MessageType:   msg.Name(),
		Msg:           a,
		Signature:     nil,
		CreatedHeight: height,                         // used for safe pruning
		Time:          uint64(time.Now().UnixMicro()), // used for hash collision entropy
		Fee:           fee,
		Memo:          memo,
		NetworkId:     networkId,
		ChainId:       chainId,
	}
	return tx, tx.Sign(pk)
}
