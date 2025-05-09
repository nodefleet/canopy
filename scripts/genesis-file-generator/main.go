package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/canopy-network/canopy/fsm"
	"github.com/canopy-network/canopy/lib"
	"github.com/canopy-network/canopy/lib/crypto"
)

var (
	defaultConfig = &lib.Config{
		MainConfig: lib.MainConfig{
			LogLevel: "debug",
			ChainId:  1,
			RootChain: []lib.RootChain{
				{
					ChainId: 1,
					Url:     "http://node-1:50002",
				},
			},
			RunVDF: true,
		},
		RPCConfig: lib.RPCConfig{
			WalletPort:   "50000",
			ExplorerPort: "50001",
			RPCPort:      "50002",
			AdminPort:    "50003",
			RPCUrl:       "http://localhost:50002",
			AdminRPCUrl:  "http://localhost:50003",
			TimeoutS:     3,
		},
		StoreConfig: lib.StoreConfig{
			DataDirPath: "/root/.canopy",
			DBName:      "canopy",
			InMemory:    false,
		},
		P2PConfig: lib.P2PConfig{
			NetworkID:       1,
			ListenAddress:   "0.0.0.0:9001",
			ExternalAddress: "node-1",
			MaxInbound:      21,
			MaxOutbound:     7,
			TrustedPeerIDs:  nil,
			DialPeers:       []string{},
			BannedPeerIDs:   nil,
			BannedIPs:       nil,
		},
		ConsensusConfig: lib.ConsensusConfig{
			ElectionTimeoutMS:       2000,
			ElectionVoteTimeoutMS:   3000,
			ProposeTimeoutMS:        3000,
			ProposeVoteTimeoutMS:    2000,
			PrecommitTimeoutMS:      2000,
			PrecommitVoteTimeoutMS:  2000,
			CommitTimeoutMS:         6000,
			RoundInterruptTimeoutMS: 2000,
		},
		MempoolConfig: lib.MempoolConfig{
			MaxTotalBytes:       1000000,
			MaxTransactionCount: 5000,
			IndividualMaxTxSize: 4000,
			DropPercentage:      35,
		},
		MetricsConfig: lib.MetricsConfig{
			Enabled:           true,
			PrometheusAddress: "0.0.0.0:9090",
		},
	}
)

type IndividualFile struct {
	Config       *lib.Config
	ValidatorKey string
	Nick         string
}

type IndividualFiles struct {
	Files []*IndividualFile
}

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

func addValidators(validators int, isDelegate, multiNode bool, nickPrefix, password string, genesis *fsm.GenesisState, keystore *crypto.Keystore, files *IndividualFiles) {
	for i := range validators {
		stakedAmount := 0
		if multiNode || (i == 0 && !isDelegate) {
			stakedAmount = 1000000000
		}
		nick := fmt.Sprintf("%s-%d", nickPrefix, i)
		pk := mustCreateKey()
		if (multiNode || i == 0) && !isDelegate {
			config := defaultConfig
			config.RootChain[0].Url = fmt.Sprintf("http://%s:50002", nick)
			config.ExternalAddress = nick
			files.Files = append(files.Files, &IndividualFile{
				Config:       config,
				ValidatorKey: pk.String(),
				Nick:         nick,
			})
		}
		genesis.Accounts = append(genesis.Accounts, &fsm.Account{
			Address: pk.PublicKey().Address().Bytes(),
			Amount:  1000000,
		})
		genesis.Validators = append(genesis.Validators, &fsm.Validator{
			Address:      pk.PublicKey().Address().Bytes(),
			PublicKey:    pk.PublicKey().Bytes(),
			Committees:   []uint64{1},
			NetAddress:   fmt.Sprintf("tcp://%s", nick),
			StakedAmount: uint64(stakedAmount),
			Output:       pk.PublicKey().Address().Bytes(),
			Delegate:     isDelegate,
		})
		mustUpdateKeystore(pk.Bytes(), nick, password, keystore)
	}
}

func mustSetDirectory(dir string) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}
}

func mustDeleteInDirectory(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		err := os.RemoveAll(filepath.Join(dir, entry.Name()))
		if err != nil {
			panic(err)
		}
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
		multiNode  = flag.Bool("multiNode", false, "Flag to create config for multiples nodes or not")
	)
	flag.Parse()

	nonSignWindow := 10
	maxNonSign := 4
	if !*multiNode {
		maxNonSign = math.MaxInt64
		nonSignWindow = math.MaxInt64
	}

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
				MaxNonSign:                         uint64(maxNonSign),
				NonSignWindow:                      uint64(nonSignWindow),
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

	files := &IndividualFiles{}

	addAccounts(*accounts, *password, genesis, keystore)
	addValidators(*validators, false, *multiNode, "validator", *password, genesis, keystore, files)
	addValidators(*delegators, true, *multiNode, "delegator", *password, genesis, keystore, files)

	mustSetDirectory(".config")
	mustDeleteInDirectory(".config")

	for _, file := range files.Files {
		nodePath := fmt.Sprintf(".config/%s", file.Nick)

		mustSetDirectory(nodePath)

		mustSaveAsJSON(fmt.Sprintf("%s/genesis.json", nodePath), genesis)
		mustSaveAsJSON(fmt.Sprintf("%s/keystore.json", nodePath), keystore)
		mustSaveAsJSON(fmt.Sprintf("%s/config.json", nodePath), file.Config)
		mustSaveAsJSON(fmt.Sprintf("%s/validator_key.json", nodePath), file.ValidatorKey)
	}
}
