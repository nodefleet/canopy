package bft

import (
	"fmt"

	"github.com/canopy-network/canopy/lib"
	"google.golang.org/protobuf/proto"
)

func ErrUnknownConsensusMsg(t proto.Message) lib.ErrorI {
	return lib.NewError(lib.CodeUnknownConsensusMessage, lib.ConsensusModule, fmt.Sprintf("unknown consensus message: %T", t))
}

func ErrDuplicateVote() lib.ErrorI {
	return lib.NewError(lib.CodeDuplicateVote, lib.ConsensusModule, "duplicate vote")
}

func ErrDuplicateProposerMessage() lib.ErrorI {
	return lib.NewError(lib.CodeDuplicateProposerMessage, lib.ConsensusModule, "duplicate proposer message")
}

func ErrUnableToAddSigner(err error) lib.ErrorI {
	return lib.NewError(lib.CodeUnableToAddSigner, lib.ConsensusModule, fmt.Sprintf("multiKey.AddSigner() failed with err: %s", err.Error()))
}

func ErrPartialSignatureEmpty() lib.ErrorI {
	return lib.NewError(lib.CodePartialSignatureEmpty, lib.ConsensusModule, "empty signature")
}

func ErrInvalidPublicKey() lib.ErrorI {
	return lib.NewError(lib.CodeInvalidPubKey, lib.ConsensusModule, "invalid public key")
}

func ErrInvalidSignatureLength() lib.ErrorI {
	return lib.NewError(lib.CodeInvalidSignatureLength, lib.ConsensusModule, "invalid signature length")
}

func ErrInvalidPartialSignature() lib.ErrorI {
	return lib.NewError(lib.CodeInvalidPartialSignature, lib.ConsensusModule, "invalid partial signature")
}

func ErrEmptyMessage() lib.ErrorI {
	return lib.NewError(lib.CodeEmptyMessage, lib.ConsensusModule, "empty consensus message")
}

func ErrNoSafeNodeJustification() lib.ErrorI {
	return lib.NewError(lib.CodeNoSafeNodeJustification, lib.ConsensusModule, "no safe node justification")
}

func ErrMismatchPublicKeys() lib.ErrorI {
	return lib.NewError(lib.CodeMismatchPublicKeys, lib.ConsensusModule, "mismatch public keys")
}

func ErrMismatchedProposals() lib.ErrorI {
	return lib.NewError(lib.CodeMismatchProposals, lib.ConsensusModule, "mismatch proposals")
}

func ErrFailedSafeNodePredicate() lib.ErrorI {
	return lib.NewError(lib.CodeFailedSafeNode, lib.ConsensusModule, "safe node failed")
}

func ErrAggregateSignature(err error) lib.ErrorI {
	return lib.NewError(lib.CodeAggregateSignature, lib.ConsensusModule, fmt.Sprintf("aggregateSignature() failed with err: %s", err.Error()))
}
