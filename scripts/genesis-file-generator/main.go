package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/canopy-network/canopy/fsm"
	"github.com/canopy-network/canopy/lib"
	"github.com/canopy-network/canopy/lib/crypto"
	"github.com/launchdarkly/go-jsonstream/v3/jwriter"
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
	Keystore     *crypto.Keystore
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

// addAccounts concurrently creates keys and accounts
func addAccounts(accounts int, wg *sync.WaitGroup, semaphoreChan chan struct{}, accountChan chan *fsm.Account) {
	for i := range accounts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			semaphoreChan <- struct{}{}
			defer func() { <-semaphoreChan }()

			nick := fmt.Sprintf("account-%d", i)

			addrStr := fmt.Sprintf("%020x", i)

			fmt.Printf("Creating key for: %s \n", nick)

			accountChan <- &fsm.Account{
				Address: []byte(addrStr),
				Amount:  1000000,
			}
		}(i)
	}
}

// addValidators concurrently creates validators and optional config
func addValidators(validators int, isDelegate, multiNode bool, nickPrefix, password string,
	files *IndividualFiles, gsync *sync.Mutex, wg *sync.WaitGroup, semaphoreChan chan struct{},
	accountChan chan *fsm.Account, validatorChan chan *fsm.Validator) {

	for i := range validators {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			semaphoreChan <- struct{}{}
			defer func() { <-semaphoreChan }()

			stakedAmount := 0
			if multiNode || (i == 0 && !isDelegate) {
				stakedAmount = 1000000000
			}
			nick := fmt.Sprintf("%s-%d", nickPrefix, i)
			pk := mustCreateKey()
			fmt.Printf("Creating key for: %s \n", nick)

			var configCopy *lib.Config
			keystore := &crypto.Keystore{
				AddressMap:  make(map[string]*crypto.EncryptedPrivateKey, 1),
				NicknameMap: make(map[string]string, 1),
			}
			if (multiNode || i == 0) && !isDelegate {
				config := *defaultConfig
				config.RootChain[0].Url = fmt.Sprintf("http://%s:50002", nick)
				config.ExternalAddress = nick
				configCopy = &config
				mustUpdateKeystore(pk.Bytes(), nick, password, keystore)
			}

			validatorChan <- &fsm.Validator{
				Address:      pk.PublicKey().Address().Bytes(),
				PublicKey:    pk.PublicKey().Bytes(),
				Committees:   []uint64{1},
				NetAddress:   fmt.Sprintf("tcp://%s", nick),
				StakedAmount: uint64(stakedAmount),
				Output:       pk.PublicKey().Address().Bytes(),
				Delegate:     isDelegate,
			}

			accountChan <- &fsm.Account{
				Address: pk.PublicKey().Address().Bytes(),
				Amount:  1000000,
			}

			if configCopy != nil {
				gsync.Lock()
				files.Files = append(files.Files, &IndividualFile{
					Config:       configCopy,
					ValidatorKey: pk.String(),
					Nick:         nick,
					Keystore:     keystore,
				})
				gsync.Unlock()
			}
		}(i)
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

func genesisWriter(multiNode bool, accountLen, validatorLen int, wg *sync.WaitGroup, accountChan chan *fsm.Account, validatorChan chan *fsm.Validator) {
	defer wg.Done()

	genesisFile, err := os.Create(".config/genesis.json")
	if err != nil {
		panic(err)
	}
	defer genesisFile.Close()

	writer := jwriter.NewStreamingWriter(genesisFile, 1024)

	obj := writer.Object()
	obj.Name("time").Int(int(time.Now().Unix()))

	fmt.Println("Starting to write validators!")

	obj.Name("validators")
	arr := writer.Array()
	for range validatorLen {
		validator := <-validatorChan
		validatorObj := writer.Object()
		validatorObj.Name("address").String(hex.EncodeToString(validator.Address))
		validatorObj.Name("publicKey").String(hex.EncodeToString(validator.PublicKey))
		validatorObj.Name("committees")
		cArr := writer.Array()
		for _, committee := range validator.Committees {
			writer.Int(int(committee))
		}
		cArr.End()
		validatorObj.Name("netAddress").String(validator.NetAddress)
		validatorObj.Name("stakedAmount").Int(int(validator.StakedAmount))
		validatorObj.Name("output").String(hex.EncodeToString(validator.Output))
		validatorObj.Name("delegate").Bool(validator.Delegate)
		validatorObj.End()
	}
	arr.End()

	fmt.Println("Starting to write accounts!")

	obj.Name("accounts")
	arr = writer.Array()
	for range accountLen {
		account := <-accountChan
		accountObj := writer.Object()
		accountObj.Name("address").String(hex.EncodeToString(account.Address))
		accountObj.Name("amount").Int(int(account.Amount))
		accountObj.End()
	}
	arr.End()

	nonSignWindow := 10
	maxNonSign := 4
	if !multiNode {
		maxNonSign = math.MaxInt64
		nonSignWindow = math.MaxInt64
	}

	remainingFields := map[string]interface{}{
		"params": &fsm.Params{
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

	for key, value := range remainingFields {
		obj.Name(key)
		data, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		writer.Raw(json.RawMessage(data))
	}

	obj.End()

	if err := writer.Flush(); err != nil {
		panic(err)
	}
}

func main() {
	fmt.Println("Deleting old files!")

	mustSetDirectory(".config")
	mustDeleteInDirectory(".config")

	fmt.Println("Creating new files!")

	var (
		delegators  = flag.Int("delegators", 10, "Number of delegators")
		validators  = flag.Int("validators", 5, "Number of validators")
		accounts    = flag.Int("accounts", 100, "Number of accounts")
		password    = flag.String("password", "pablito", "Password for keystore")
		multiNode   = flag.Bool("multiNode", false, "Flag to create config for multiples nodes or not")
		concurrency = flag.Int64("concurrency", 100, "Concurrency of the processes")
		buffer      = flag.Int64("buffer", 0, "Buffer of validators to be saved while waiting processing")
	)
	flag.Parse()

	acountsLen := int64(*delegators + *validators + *accounts)
	validatorsLen := int64(*delegators + *validators)

	if *buffer == 0 {
		buffer = &validatorsLen
	}

	accountChan := make(chan *fsm.Account, acountsLen) // this needs to always be the accounts len bc it starts writing after the validators
	validatorChan := make(chan *fsm.Validator, *buffer)

	var genesisWG sync.WaitGroup

	genesisWG.Add(1)
	go genesisWriter(*multiNode, int(acountsLen), int(validatorsLen), &genesisWG, accountChan, validatorChan)

	files := &IndividualFiles{}

	semaphoreChan := make(chan struct{}, *concurrency)
	var gsync sync.Mutex
	var wg sync.WaitGroup

	addValidators(*validators, false, *multiNode, "validator", *password, files, &gsync, &wg, semaphoreChan, accountChan, validatorChan)
	addValidators(*delegators, true, *multiNode, "delegator", *password, files, &gsync, &wg, semaphoreChan, accountChan, validatorChan)
	addAccounts(*accounts, &wg, semaphoreChan, accountChan)

	wg.Wait()
	genesisWG.Wait()

	for _, file := range files.Files {
		nodePath := fmt.Sprintf(".config/%s", file.Nick)

		mustSetDirectory(nodePath)

		input, err := os.ReadFile(".config/genesis.json")
		if err != nil {
			panic(err)
		}

		err = os.WriteFile(fmt.Sprintf("%s/genesis.json", nodePath), input, 0644)
		if err != nil {
			panic(err)
		}

		mustSaveAsJSON(fmt.Sprintf("%s/keystore.json", nodePath), file.Keystore)
		mustSaveAsJSON(fmt.Sprintf("%s/config.json", nodePath), file.Config)
		mustSaveAsJSON(fmt.Sprintf("%s/validator_key.json", nodePath), file.ValidatorKey)
	}
}
