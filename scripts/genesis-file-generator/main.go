package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/canopy-network/canopy/fsm"
	"github.com/canopy-network/canopy/lib/crypto"
)

func mustCreateKey() crypto.PrivateKeyI {
	pk, err := crypto.NewBLS12381PrivateKey()
	if err != nil {
		panic(err)
	}

	return pk
}

func mustUpdateKeystore(privateKey []byte, nickName, password string, keystore *crypto.Keystore) {
	_, err := keystore.ImportRaw(privateKey, password, crypto.ImportRawOpts{
		Nickname: nickName,
	})
	if err != nil {
		panic(err)
	}
}

func addAccounts(accounts int, password string, genesis *fsm.GenesisState, keystore *crypto.Keystore) {
	for i := range accounts {
		pk := mustCreateKey()
		genesis.Accounts = append(genesis.Accounts, &fsm.Account{
			Address: pk.PublicKey().Address().Bytes(),
			Amount:  1000000,
		})
		mustUpdateKeystore(pk.Bytes(), fmt.Sprintf("account-%d", i), password, keystore)
	}
}

func addValidators(validators int, isDelegate bool, nickPrefix, password string, genesis *fsm.GenesisState, keystore *crypto.Keystore) {
	for i := range validators {
		pk := mustCreateKey()
		genesis.Accounts = append(genesis.Accounts, &fsm.Account{
			Address: pk.PublicKey().Address().Bytes(),
			Amount:  1000000,
		})
		genesis.Validators = append(genesis.Validators, &fsm.Validator{
			Address:      pk.PublicKey().Address().Bytes(),
			PublicKey:    pk.PublicKey().Bytes(),
			Committees:   []uint64{1},
			NetAddress:   fmt.Sprintf("tcp://%s-%d", nickPrefix, i),
			StakedAmount: 1000000000,
			Output:       pk.PublicKey().Address().Bytes(),
			Delegate:     isDelegate,
		})
		mustUpdateKeystore(pk.Bytes(), fmt.Sprintf("%s-%d", nickPrefix, i), password, keystore)
	}
}

func mustSaveAsJSON(filename string, data any) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	err = encoder.Encode(data)
	if err != nil {
		panic(err)
	}
}

func main() {
	var (
		delegators = flag.Int("delegators", 10, "Number of delegators")
		validators = flag.Int("validators", 5, "Number of validators")
		accounts   = flag.Int("accounts", 100, "Number of accounts")
		password   = flag.String("password", "pablito", "Password for keystore")
	)
	flag.Parse()

	genesis := &fsm.GenesisState{
		Time: uint64(time.Now().Unix()),
		Params: &fsm.Params{
			Consensus: &fsm.ConsensusParams{
				BlockSize:       1000000,
				ProtocolVersion: "1/0",
				RootChainId:     1,
				Retired:         0,
			},
			Validator: &fsm.ValidatorParams{
				UnstakingBlocks:                    2,
				MaxPauseBlocks:                     4380,
				DoubleSignSlashPercentage:          10,
				NonSignSlashPercentage:             1,
				MaxNonSign:                         4,
				NonSignWindow:                      10,
				MaxCommittees:                      15,
				MaxCommitteeSize:                   100,
				EarlyWithdrawalPenalty:             20,
				DelegateUnstakingBlocks:            2,
				MinimumOrderSize:                   1000,
				StakePercentForSubsidizedCommittee: 33,
				MaxSlashPerCommittee:               15,
				DelegateRewardPercentage:           10,
				BuyDeadlineBlocks:                  15,
				LockOrderFeeMultiplier:             2,
			},
			Fee: &fsm.FeeParams{
				SendFee:            10000,
				StakeFee:           10000,
				EditStakeFee:       10000,
				UnstakeFee:         10000,
				PauseFee:           10000,
				UnpauseFee:         10000,
				ChangeParameterFee: 10000,
				DaoTransferFee:     10000,
				SubsidyFee:         10000,
				CreateOrderFee:     10000,
				EditOrderFee:       10000,
				DeleteOrderFee:     10000,
			},
			Governance: &fsm.GovernanceParams{
				DaoRewardPercentage: 10,
			},
		},
	}

	keystore := &crypto.Keystore{
		AddressMap:  make(map[string]*crypto.EncryptedPrivateKey, *accounts+*delegators+*validators),
		NicknameMap: make(map[string]string, *delegators+*validators),
	}

	addAccounts(*accounts, *password, genesis, keystore)
	addValidators(*validators, false, "validator", *password, genesis, keystore)
	addValidators(*delegators, true, "delegator", *password, genesis, keystore)

	mustSaveAsJSON("genesis.json", genesis)
	mustSaveAsJSON("keystore.json", keystore)
}
