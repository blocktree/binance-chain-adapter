/*
 * Copyright 2018 The openwallet Authors
 * This file is part of the openwallet library.
 *
 * The openwallet library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The openwallet library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 */

package binancechain

import (
	"encoding/hex"
	"fmt"
	"github.com/binance-chain/go-sdk/types/tx"
	"github.com/blocktree/go-owcdrivers/addressEncoder"
	"github.com/blocktree/go-owcdrivers/binancechainTransaction"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	ow "github.com/blocktree/openwallet/common"
	"github.com/blocktree/openwallet/log"
	"github.com/blocktree/openwallet/openwallet"
)

type TransactionDecoder struct {
	openwallet.TransactionDecoderBase
	wm *WalletManager //钱包管理者
}

//NewTransactionDecoder 交易单解析器
func NewTransactionDecoder(wm *WalletManager) *TransactionDecoder {
	decoder := TransactionDecoder{}
	decoder.wm = wm
	return &decoder
}

//CreateRawTransaction 创建交易单
func (decoder *TransactionDecoder) CreateRawTransaction(wrapper openwallet.WalletDAI, rawTx *openwallet.RawTransaction) error {
	if rawTx.Coin.IsContract {
		return decoder.CreateBNBRawTransaction(wrapper, rawTx)
	}
	return openwallet.Errorf(openwallet.ErrCreateRawTransactionFailed, "[%s] Miss contract details to create transaction!", rawTx.Account.AccountID)
}

//SignRawTransaction 签名交易单
func (decoder *TransactionDecoder) SignRawTransaction(wrapper openwallet.WalletDAI, rawTx *openwallet.RawTransaction) error {
	if rawTx.Coin.IsContract {
		return decoder.SignBNBRawTransaction(wrapper, rawTx)
	}
	return openwallet.Errorf(openwallet.ErrSignRawTransactionFailed, "[%s] Miss contract details to sign transaction!", rawTx.Account.AccountID)
}

//VerifyRawTransaction 验证交易单，验证交易单并返回加入签名后的交易单
func (decoder *TransactionDecoder) VerifyRawTransaction(wrapper openwallet.WalletDAI, rawTx *openwallet.RawTransaction) error {
	if rawTx.Coin.IsContract {
		return decoder.VerifyBNBRawTransaction(wrapper, rawTx)
	}
	return openwallet.Errorf(openwallet.ErrVerifyRawTransactionFailed, "[%s] Miss contract details to verify transaction!", rawTx.Account.AccountID)
}

func (decoder *TransactionDecoder) SubmitRawTransaction(wrapper openwallet.WalletDAI, rawTx *openwallet.RawTransaction) (*openwallet.Transaction, error) {
	if len(rawTx.RawHex) == 0 {
		return nil, fmt.Errorf("transaction hex is empty")
	}

	if !rawTx.IsCompleted {
		return nil, fmt.Errorf("transaction is not completed validation")
	}

	txid, err := decoder.wm.SendRawTransaction(rawTx.RawHex)
	if err != nil {
		fmt.Println("Tx to send: ", rawTx.RawHex)
		return nil, err
	} else {
		txBytes, _ := hex.DecodeString(rawTx.RawHex)
		trx, _ := binancechainTransaction.DecodeRawTransaction(txBytes)
		sequence := trx.Signatures[0].Sequence + 1

		hash := trx.Signatures[0].Address().Bytes()

		address := addressEncoder.AddressEncode(hash, addressEncoder.BNB_mainnetAddress)

		wrapper.SetAddressExtParam(address, decoder.wm.FullName(), sequence)
	}

	rawTx.TxID = txid
	rawTx.IsSubmit = true

	tx := openwallet.Transaction{
		From:       rawTx.TxFrom,
		To:         rawTx.TxTo,
		Amount:     rawTx.TxAmount,
		Coin:       rawTx.Coin,
		TxID:       rawTx.TxID,
		Decimal:    int32(rawTx.Coin.Contract.Decimals),
		AccountID:  rawTx.Account.AccountID,
		Fees:       rawTx.Fees,
		SubmitTime: time.Now().Unix(),
	}

	tx.WxID = openwallet.GenTransactionWxID(&tx)

	return &tx, nil
}

func (decoder *TransactionDecoder) CreateBNBRawTransaction(wrapper openwallet.WalletDAI, rawTx *openwallet.RawTransaction) error {

	addresses, err := wrapper.GetAddressList(0, -1, "AccountID", rawTx.Account.AccountID)

	if err != nil {
		return err
	}

	if len(addresses) == 0 {
		return openwallet.Errorf(openwallet.ErrAccountNotAddress, "[%s] have not addresses", rawTx.Account.AccountID)
	}

	addressesBalanceList := make([]AddrBalance, 0, len(addresses))

	for i, addr := range addresses {
		balance, err := decoder.wm.RpcClient.getBalance(addr.Address, rawTx.Coin.Contract.Address)

		if err != nil {
			return err
		}

		balance.index = i
		addressesBalanceList = append(addressesBalanceList, *balance)
	}

	sort.Slice(addressesBalanceList, func(i int, j int) bool {
		return addressesBalanceList[i].Balance.Cmp(addressesBalanceList[j].Balance) >= 0
	})

	fee, err := decoder.wm.RpcClient.getFeeByHeight(0)
	if err != nil {
		return openwallet.Errorf(openwallet.ErrUnknownException, "[%s] Failed to get current fee!", rawTx.Account.AccountID)
	}

	var amountStr, to string
	for k, v := range rawTx.To {
		to = k
		amountStr = v
		break
	}

	amount := big.NewInt(int64(convertFromAmount(amountStr, rawTx.Coin.Contract.Decimals)))
	if rawTx.Coin.Contract.Address == "BNB" {
		amount = amount.Add(amount, big.NewInt(int64(fee)))
	}

	from := ""
	avaliable := ""
	count := big.NewInt(0)
	countList := []uint64{}
	for _, a := range addressesBalanceList {
		if a.Balance.Cmp(amount) < 0 {
			count.Add(count, a.Balance)
			if count.Cmp(amount) >= 0 {
				countList = append(countList, a.Balance.Sub(a.Balance, count.Sub(count, amount)).Uint64())
				log.Error("The " + rawTx.Coin.Contract.Address + " of the account is enough,"+
					" but cannot be sent in just one transaction!\n"+
					"the amount can be sent in "+string(len(countList))+
					"times with amounts :\n"+strings.Replace(strings.Trim(fmt.Sprint(countList), "[]"), " ", ",", -1), err)
				return err
			} else {
				countList = append(countList, a.Balance.Uint64())
			}
			continue
		}
		if rawTx.Coin.Contract.Address != "BNB" {
			feeBalance, err := decoder.wm.RpcClient.getBalance(a.Address, "BNB")
			if err != nil {
				return openwallet.Errorf(openwallet.ErrUnknownException, "[%s] Failed to get BNB balance!", a.Address)
			}

			if feeBalance.Balance.Uint64() < fee {
				avaliable = a.Address
				continue
			}
		}

		from = a.Address
		break
	}

	if from == "" {
		if avaliable != "" {
			return openwallet.Errorf(openwallet.ErrInsufficientFees, "the " + rawTx.Coin.Contract.Address + " balance of address: %s is enough, but which has not enough BNB as fee!", avaliable)
		}
		return openwallet.Errorf(openwallet.ErrInsufficientBalanceOfAccount, "the balance: %s is not enough", amountStr)
	}

	rawTx.TxFrom = []string{from}
	rawTx.TxTo = []string{to}
	rawTx.TxAmount = amountStr
	rawTx.Fees = convertToAmount(fee, 8)
	rawTx.FeeRate = convertToAmount(fee, 8)

	accountNumber, sequenceChain, err := decoder.wm.RpcClient.getAccountNumberAndSequence(from)
	if err != nil {
		return openwallet.Errorf(openwallet.ErrUnknownException, "Failed to get account number and sequence of address: %s !!", from)
	}

	var sequence uint64
	sequence_db, err := wrapper.GetAddressExtParam(from, decoder.wm.FullName())
	if err != nil {
		return err
	}

	if sequence_db == nil {
		sequence = 0
	} else {
		sequence = ow.NewString(sequence_db).UInt64()
	}

	if sequenceChain > int64(sequence) {
		sequence = uint64(sequenceChain)
	}
	memo := rawTx.GetExtParam().Get("memo").String()
	emptyTrans, hash, err := binancechainTransaction.CreateEmptyTransactionAndHash(from, to,rawTx.Coin.Contract.Address, int64(convertFromAmount(amountStr, rawTx.Coin.Contract.Decimals)), accountNumber, int64(sequence), tx.Source, memo)
	if err != nil {
		return openwallet.Errorf(openwallet.ErrCreateRawTransactionFailed, "Failed to create transaction : %s !!", rawTx.Account.AccountID)
	}

	rawTx.RawHex = emptyTrans

	if rawTx.Signatures == nil {
		rawTx.Signatures = make(map[string][]*openwallet.KeySignature)
	}

	keySigs := make([]*openwallet.KeySignature, 0)

	addr, err := wrapper.GetAddress(from)
	if err != nil {
		return err
	}
	signature := openwallet.KeySignature{
		EccType: decoder.wm.Config.CurveType,
		Nonce:   "",
		Address: addr,
		Message: hash,
	}

	keySigs = append(keySigs, &signature)

	rawTx.Signatures[rawTx.Account.AccountID] = keySigs

	rawTx.FeeRate = big.NewInt(int64(fee)).String()

	rawTx.IsBuilt = true

	return nil
}

func (decoder *TransactionDecoder) SignBNBRawTransaction(wrapper openwallet.WalletDAI, rawTx *openwallet.RawTransaction) error {
	key, err := wrapper.HDKey()
	if err != nil {
		return nil
	}

	keySignatures := rawTx.Signatures[rawTx.Account.AccountID]

	if keySignatures != nil {
		for _, keySignature := range keySignatures {

			childKey, err := key.DerivedKeyWithPath(keySignature.Address.HDPath, keySignature.EccType)
			keyBytes, err := childKey.GetPrivateKeyBytes()
			if err != nil {
				return err
			}
			//签名交易
			/////////交易单哈希签名
			sig, err := binancechainTransaction.SignRawTransaction(keySignature.Message, keyBytes)
			if err != nil {
				return fmt.Errorf("transaction hash sign failed, unexpected error: %v", err)
			} else {

				//for i, s := range sigPub {
				//	log.Info("第", i+1, "个签名结果")
				//	log.Info()
				//	log.Info("对应的公钥为")
				//	log.Info(hex.EncodeToString(s.Pubkey))
				//}

				// txHash.Normal.SigPub = *sigPub
			}

			keySignature.Signature = hex.EncodeToString(sig)
		}
	}

	log.Info("transaction hash sign success")

	rawTx.Signatures[rawTx.Account.AccountID] = keySignatures

	return nil
}

func (decoder *TransactionDecoder) VerifyBNBRawTransaction(wrapper openwallet.WalletDAI, rawTx *openwallet.RawTransaction) error {

	var (
		emptyTrans = rawTx.RawHex

		signature = ""
		pubkey    = ""
	)

	for accountID, keySignatures := range rawTx.Signatures {
		log.Debug("accountID Signatures:", accountID)
		for _, keySignature := range keySignatures {

			signature = keySignature.Signature
			pubkey = keySignature.Address.PublicKey

			//log.Debug("Signature:", signature)
			//log.Debug("PublicKey:", pubkey)
		}
	}

	pass, signedTrans := binancechainTransaction.VerifyAndCombineRawTransaction(emptyTrans, signature, pubkey)

	if pass {
		log.Debug("transaction verify passed")
		rawTx.IsCompleted = true
		rawTx.RawHex = signedTrans
	} else {
		log.Debug("transaction verify failed")
		rawTx.IsCompleted = false
	}

	return nil
}

func (decoder *TransactionDecoder) GetRawTransactionFeeRate() (feeRate string, unit string, err error) {
	fee, err := decoder.wm.RpcClient.getFeeByHeight(0)
	if err != nil {
		return "", "", err
	}
	return strconv.FormatInt(int64(fee), 10), "TX", nil
}

//CreateSummaryRawTransaction 创建汇总交易，返回原始交易单数组
func (decoder *TransactionDecoder) CreateSummaryRawTransaction(wrapper openwallet.WalletDAI, sumRawTx *openwallet.SummaryRawTransaction) ([]*openwallet.RawTransaction, error) {
	if sumRawTx.Coin.IsContract {
		return decoder.CreateTokenSummaryRawTransaction(wrapper, sumRawTx)
	}
	return nil, openwallet.Errorf(openwallet.ErrSubmitRawTransactionFailed, "[%s] Miss contract details to summary!", sumRawTx.Account.AccountID)
}

func (decoder *TransactionDecoder) CreateTokenSummaryRawTransaction(wrapper openwallet.WalletDAI, sumRawTx *openwallet.SummaryRawTransaction) ([]*openwallet.RawTransaction, error) {

	var (
		rawTxArray      = make([]*openwallet.RawTransaction, 0)
		accountID       = sumRawTx.Account.AccountID
		minTransfer     = big.NewInt(int64(convertFromAmount(sumRawTx.MinTransfer, sumRawTx.Coin.Contract.Decimals)))
		retainedBalance = big.NewInt(int64(convertFromAmount(sumRawTx.RetainedBalance, sumRawTx.Coin.Contract.Decimals)))
	)

	if minTransfer.Cmp(retainedBalance) < 0 {
		return nil, fmt.Errorf("mini transfer amount must be greater than address retained balance")
	}

	//获取wallet
	addresses, err := wrapper.GetAddressList(sumRawTx.AddressStartIndex, sumRawTx.AddressLimit,
		"AccountID", sumRawTx.Account.AccountID)
	if err != nil {
		return nil, err
	}

	if len(addresses) == 0 {
		return nil, fmt.Errorf("[%s] have not addresses", accountID)
	}

	searchAddrs := make([]string, 0)
	addrBalanceArray := make([]*AddrBalance, 0)
	for _, address := range addresses {
		searchAddrs = append(searchAddrs, address.Address)
		balance, err := decoder.wm.RpcClient.getBalance(address.Address, sumRawTx.Coin.Contract.Address)
		if err != nil {
			return nil, err
		}
		addrBalanceArray = append(addrBalanceArray, balance)
	}

	for _, addrBalance := range addrBalanceArray {

		//检查余额是否超过最低转账
		addrBalance_BI := addrBalance.Balance

		if addrBalance_BI.Cmp(minTransfer) < 0 {
			continue
		}
		//计算汇总数量 = 余额 - 保留余额
		sumAmount_BI := new(big.Int)
		sumAmount_BI.Sub(addrBalance_BI, retainedBalance)

		//this.wm.Log.Debug("sumAmount:", sumAmount)
		//计算手续费
		feeValue, err := decoder.wm.RpcClient.getFeeByHeight(0)
		if err != nil {
			return nil, openwallet.Errorf(openwallet.ErrUnknownException, "[%s] Failed to get current fee!", sumRawTx.Account.AccountID)
		}
		fee := big.NewInt(int64(feeValue)) //(int64(decoder.wm.Config.FeeCharge))


		//减去手续费
		if sumRawTx.Coin.Contract.Address == "BNB" {
			sumAmount_BI.Sub(sumAmount_BI, fee)
			if sumAmount_BI.Cmp(big.NewInt(0)) <= 0 {
				continue
			}
		}


		sumAmount := convertToAmount(sumAmount_BI.Uint64(), sumRawTx.Coin.Contract.Decimals)
		fees := convertToAmount(fee.Uint64(), 8)

		log.Debugf("balance: %v", convertToAmount(addrBalance.Balance.Uint64(), sumRawTx.Coin.Contract.Decimals))
		log.Debugf("fees: %v", fees)
		log.Debugf("sumAmount: %v", sumAmount)

		//创建一笔交易单
		rawTx := &openwallet.RawTransaction{
			Coin:    sumRawTx.Coin,
			Account: sumRawTx.Account,
			To: map[string]string{
				sumRawTx.SummaryAddress: sumAmount,
			},
			Required: 1,
		}

		createErr := decoder.createRawTransaction(
			wrapper,
			rawTx,
			addrBalance.Address,
			feeValue)
		if createErr != nil {
			return nil, createErr
		}

		//创建成功，添加到队列
		rawTxArray = append(rawTxArray, rawTx)

	}
	return rawTxArray, nil
}

func (decoder *TransactionDecoder) createRawTransaction(wrapper openwallet.WalletDAI, rawTx *openwallet.RawTransaction, from string, feeValue uint64) error {
	var amountStr, to string
	for k, v := range rawTx.To {
		to = k
		amountStr = v
		break
	}

	amount := convertFromAmount(amountStr, rawTx.Coin.Contract.Decimals)
	fromAddr, err := wrapper.GetAddress(from)
	if err != nil {
		return err
	}
	//fromPubkey := fromAddr.PublicKey

	rawTx.TxFrom = []string{from}
	rawTx.TxTo = []string{to}
	rawTx.TxAmount = amountStr
	rawTx.Fees = convertToAmount(feeValue, 8)
	rawTx.FeeRate = rawTx.Fees

	var sequence uint64
	sequence_db, err := wrapper.GetAddressExtParam(from, decoder.wm.FullName())
	if err != nil {
		return err
	}
	if sequence_db == nil {
		sequence = 0
	} else {
		sequence = ow.NewString(sequence_db).UInt64()
	}
	accountNumber, sequenceChain, err := decoder.wm.RpcClient.getAccountNumberAndSequence(from)
	if err != nil {
		return err
	}
	if sequenceChain > int64(sequence) {
		sequence = uint64(sequenceChain)
	}
	memo := rawTx.GetExtParam().Get("memo").String()


	emptyTrans, hash, err := binancechainTransaction.CreateEmptyTransactionAndHash(from, to, rawTx.Coin.Contract.Address, int64(amount), accountNumber, int64(sequence), tx.Source, memo)
	if err != nil {
		return err
	}
	rawTx.RawHex = emptyTrans

	if rawTx.Signatures == nil {
		rawTx.Signatures = make(map[string][]*openwallet.KeySignature)
	}

	keySigs := make([]*openwallet.KeySignature, 0)

	signature := openwallet.KeySignature{
		EccType: decoder.wm.Config.CurveType,
		Nonce:   "",
		Address: fromAddr,
		Message: hash,
	}

	keySigs = append(keySigs, &signature)

	rawTx.Signatures[rawTx.Account.AccountID] = keySigs

	rawTx.FeeRate = convertToAmount(feeValue, 8)

	rawTx.IsBuilt = true

	return nil
}

//CreateSummaryRawTransactionWithError 创建汇总交易，返回能原始交易单数组（包含带错误的原始交易单）
func (decoder *TransactionDecoder) CreateSummaryRawTransactionWithError(wrapper openwallet.WalletDAI, sumRawTx *openwallet.SummaryRawTransaction) ([]*openwallet.RawTransactionWithError, error) {
	raTxWithErr := make([]*openwallet.RawTransactionWithError, 0)
	rawTxs, err := decoder.CreateSummaryRawTransaction(wrapper, sumRawTx)
	if err != nil {
		return nil, err
	}
	for _, tx := range rawTxs {
		raTxWithErr = append(raTxWithErr, &openwallet.RawTransactionWithError{
			RawTx: tx,
			Error: nil,
		})
	}
	return raTxWithErr, nil
}
