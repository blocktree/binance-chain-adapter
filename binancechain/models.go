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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/blocktree/go-owcdrivers/binancechainTransaction"
	"time"

	"github.com/blocktree/go-owcrypt"
	"github.com/blocktree/openwallet/crypto"
	"github.com/blocktree/openwallet/openwallet"
	"github.com/ethereum/go-ethereum/common"
	"github.com/tidwall/gjson"
)

type Block struct {
	Hash          string
	VersionBlock  byte
	VersionApp    byte
	ChainID       string
	Height        uint64
	Timestamp     uint64
	PrevBlockHash string
	Transactions  []string
}


type FeeValue struct {
	Amount uint64
	//	Denom  string
}

type AddrAmount struct {
	Address string
	Amount uint64
}
type TxDetail struct {
	Denom string
	From []AddrAmount
	To []AddrAmount
}

type Transaction struct {
	TxID        string
	BlockHeight uint64
	Memo        string
	TxDetails   map[string](*TxDetail)
}


func NewTransaction(json *gjson.Result) *Transaction {
	obj := Transaction{}
	obj.TxDetails = make(map[string](*TxDetail))

	base64decoder := base64.StdEncoding

	trxBytes, _ := base64decoder.DecodeString(json.Get("tx").String())
	trx, err := binancechainTransaction.DecodeRawTransaction(trxBytes)
	if err != nil {
		return nil
	}

	for _, input := range gjson.Get(string(trx.GetMsgs()[0].GetSignBytes()), "inputs").Array() {
		for _, coin := range input.Get("coins").Array() {
			denom := coin.Get("denom").String()
			if obj.TxDetails[denom] == nil {
				obj.TxDetails[denom] = &TxDetail{}
				obj.TxDetails[denom].Denom = denom
			}
			obj.TxDetails[denom].From = append(obj.TxDetails[denom].From, AddrAmount{input.Get("address").String(), coin.Get("amount").Uint()})
		}
	}

	for _, output := range gjson.Get(string(trx.GetMsgs()[0].GetSignBytes()), "outputs").Array() {
		for _, coin := range output.Get("coins").Array() {
			denom := coin.Get("denom").String()
			if obj.TxDetails[denom] == nil {
				obj.TxDetails[denom] = &TxDetail{}
				obj.TxDetails[denom].Denom = denom
			}
			obj.TxDetails[denom].To = append(obj.TxDetails[denom].To, AddrAmount{output.Get("address").String(), coin.Get("amount").Uint()})
		}
	}

	obj.Memo = trx.Memo
	obj.TxID = json.Get("hash").String()
	obj.BlockHeight = json.Get("height").Uint()

	return &obj
}


func NewBlock(json *gjson.Result) *Block {
	obj := &Block{}

	// 解析
	obj.Hash = gjson.Get(json.Raw, "block_meta").Get("block_id").Get("hash").String()
	obj.VersionBlock = byte(gjson.Get(json.Raw, "block_meta").Get("header").Get("version").Get("block").Uint())
	obj.VersionApp = byte(gjson.Get(json.Raw, "block_meta").Get("header").Get("version").Get("app").Uint())
	obj.ChainID = gjson.Get(json.Raw, "block_meta").Get("header").Get("chain_id").String()
	obj.Height = gjson.Get(json.Raw, "block_meta").Get("header").Get("height").Uint()
	timestamp, _ := time.Parse(time.RFC3339Nano, gjson.Get(json.Raw, "block_meta").Get("header").Get("time").String())
	obj.Timestamp = uint64(timestamp.Unix())
	obj.PrevBlockHash = gjson.Get(json.Raw, "block_meta").Get("header").Get("last_block_id").Get("hash").String()

	if gjson.Get(json.Raw, "block_meta").Get("header").Get("num_txs").Uint() != 0 {
		txs := gjson.Get(json.Raw, "block").Get("data").Get("txs").Array()

		for _, tx := range txs {
			txid, _ := base64.StdEncoding.DecodeString(tx.String())
			obj.Transactions = append(obj.Transactions, hex.EncodeToString(owcrypt.Hash(txid, 0, owcrypt.HASH_ALG_SHA256)))
		}
	}

	return obj
}

//BlockHeader 区块链头
func (b *Block) BlockHeader() *openwallet.BlockHeader {

	obj := openwallet.BlockHeader{}
	//解析json
	obj.Hash = b.Hash
	//obj.Confirmations = b.Confirmations
	//	obj.Merkleroot = b.TransactionMerkleRoot
	obj.Previousblockhash = b.PrevBlockHash
	obj.Height = b.Height
	//obj.Version = uint64(b.Version)
	obj.Time = b.Timestamp
	obj.Symbol = Symbol

	return &obj
}

//UnscanRecords 扫描失败的区块及交易
type UnscanRecord struct {
	ID          string `storm:"id"` // primary key
	BlockHeight uint64
	TxID        string
	Reason      string
}

func NewUnscanRecord(height uint64, txID, reason string) *UnscanRecord {
	obj := UnscanRecord{}
	obj.BlockHeight = height
	obj.TxID = txID
	obj.Reason = reason
	obj.ID = common.Bytes2Hex(crypto.SHA256([]byte(fmt.Sprintf("%d_%s", height, txID))))
	return &obj
}
