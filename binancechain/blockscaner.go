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
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/asdine/storm"
	"github.com/blocktree/openwallet/common"
	"github.com/blocktree/openwallet/openwallet"
	gosocketio "github.com/graarh/golang-socketio"
	"github.com/graarh/golang-socketio/transport"
	"github.com/shopspring/decimal"
)

const (
	blockchainBucket  = "blockchain" //区块链数据集合
	maxExtractingSize = 20           //并发的扫描线程数
)

//BNBBlockScanner bnb的区块链扫描器
type BNBBlockScanner struct {
	*openwallet.BlockScannerBase

	CurrentBlockHeight   uint64             //当前区块高度
	extractingCH         chan struct{}      //扫描工作令牌
	wm                   *WalletManager     //钱包管理者
	IsScanMemPool        bool               //是否扫描交易池
	RescanLastBlockCount uint64             //重扫上N个区块数量
	socketIO             *gosocketio.Client //socketIO客户端
	RPCServer            int
}

//ExtractResult 扫描完成的提取结果
type ExtractResult struct {
	extractData map[string]*openwallet.TxExtractData
	TxID        string
	BlockHeight uint64
	Success     bool
}

//SaveResult 保存结果
type SaveResult struct {
	TxID        string
	BlockHeight uint64
	Success     bool
}

//NewBNBBlockScanner 创建区块链扫描器
func NewBNBBlockScanner(wm *WalletManager) *BNBBlockScanner {
	bs := BNBBlockScanner{
		BlockScannerBase: openwallet.NewBlockScannerBase(),
	}

	bs.extractingCH = make(chan struct{}, maxExtractingSize)
	bs.wm = wm
	bs.IsScanMemPool = false
	bs.RescanLastBlockCount = 1

	//设置扫描任务
	bs.SetTask(bs.ScanBlockTask)

	return &bs
}

//SetRescanBlockHeight 重置区块链扫描高度
func (bs *BNBBlockScanner) SetRescanBlockHeight(height uint64) error {
	height = height - 1
	if height < 0 {
		return errors.New("block height to rescan must greater than 0.")
	}

	hash, err := bs.wm.GetBlockHash(height)
	if err != nil {
		return err
	}

	bs.wm.SaveLocalNewBlock(height, hash)

	return nil
}

//ScanBlockTask 扫描任务
func (bs *BNBBlockScanner) ScanBlockTask() {

	//获取本地区块高度
	blockHeader, err := bs.GetScannedBlockHeader()
	if err != nil {
		bs.wm.Log.Std.Info("block scanner can not get new block height; unexpected error: %v", err)
		return
	}

	currentHeight := blockHeader.Height
	currentHash := blockHeader.Hash
	var previousHeight uint64 = 0

	for {

		if !bs.Scanning {
			//区块扫描器已暂停，马上结束本次任务
			return
		}

		//获取最大高度
		maxHeight, err := bs.wm.GetBlockHeight()
		if err != nil {
			//下一个高度找不到会报异常
			bs.wm.Log.Std.Info("block scanner can not get rpc-server block height; unexpected error: %v", err)
			break
		}

		//是否已到最新高度
		if currentHeight >= maxHeight {
			bs.wm.Log.Std.Info("block scanner has scanned full chain data. Current height: %d", maxHeight)
			break
		}

		//继续扫描下一个区块
		currentHeight = currentHeight + 1
		bs.wm.Log.Std.Info("block scanner scanning height: %d ...", currentHeight)

		localBlock, err := bs.wm.RpcClient.getBlockByHeight(currentHeight)
		if err != nil {
			bs.wm.Log.Std.Info("getBlockByHeight failed; unexpected error: %v", err)
			break
		}

		isFork := false

		//判断hash是否上一区块的hash
		if currentHash != localBlock.PrevBlockHash {
			previousHeight = currentHeight - 1
			bs.wm.Log.Std.Info("block has been fork on height: %d.", currentHeight)
			bs.wm.Log.Std.Info("block height: %d local hash = %s ", previousHeight, currentHash)
			bs.wm.Log.Std.Info("block height: %d mainnet hash = %s ", previousHeight, localBlock.PrevBlockHash)

			bs.wm.Log.Std.Info("delete recharge records on block height: %d.", previousHeight)

			//删除上一区块链的所有充值记录
			//bs.DeleteRechargesByHeight(currentHeight - 1)
			forkBlock, _ := bs.wm.GetLocalBlock(previousHeight)
			//删除上一区块链的未扫记录
			bs.wm.DeleteUnscanRecord(previousHeight)
			currentHeight = previousHeight - 1 //倒退2个区块重新扫描
			if currentHeight <= 0 {
				currentHeight = 1
			}

			localBlock, err = bs.wm.GetLocalBlock(currentHeight)
			if err != nil && err != storm.ErrNotFound {
				bs.wm.Log.Std.Error("block scanner can not get local block; unexpected error: %v", err)
				break
			} else if err == storm.ErrNotFound {
				//查找core钱包的RPC
				bs.wm.Log.Info("block scanner prev block height:", currentHeight)

				localBlock, err = bs.wm.RpcClient.getBlockByHeight(currentHeight)
				if err != nil {
					bs.wm.Log.Std.Error("block scanner can not get prev block; unexpected error: %v", err)
					break
				}

			}

			//重置当前区块的hash
			currentHash = localBlock.Hash

			bs.wm.Log.Std.Info("rescan block on height: %d, hash: %s .", currentHeight, currentHash)

			//重新记录一个新扫描起点
			bs.wm.SaveLocalNewBlock(localBlock.Height, localBlock.Hash)

			isFork = true

			if forkBlock != nil {
				//通知分叉区块给观测者，异步处理
				bs.newBlockNotify(forkBlock, isFork)
			}

		} else {

			err = bs.BatchExtractTransaction(localBlock.Height, localBlock.Hash, localBlock.Transactions, false)
			if err != nil {
				bs.wm.Log.Std.Info("block scanner can not extractRechargeRecords; unexpected error: %v", err)
			}

			//重置当前区块的hash
			currentHash = localBlock.Hash

			//保存本地新高度
			bs.wm.SaveLocalNewBlock(currentHeight, currentHash)
			bs.wm.SaveLocalBlock(localBlock)

			isFork = false

			//通知新区块给观测者，异步处理
			bs.newBlockNotify(localBlock, isFork)
		}

	}

	//重扫前N个块，为保证记录找到
	for i := currentHeight - bs.RescanLastBlockCount; i < currentHeight; i++ {
		bs.scanBlock(i)
	}

	if bs.IsScanMemPool {
		//扫描交易内存池
		bs.ScanTxMemPool()
	}

	//重扫失败区块
	bs.RescanFailedRecord()

}

//ScanBlock 扫描指定高度区块
func (bs *BNBBlockScanner) ScanBlock(height uint64) error {

	block, err := bs.scanBlock(height)
	if err != nil {
		return err
	}
	bs.newBlockNotify(block, false)
	return nil
}

func (bs *BNBBlockScanner) scanBlock(height uint64) (*Block, error) {

	block, err := bs.wm.RpcClient.getBlockByHeight(height)

	if err != nil {
		bs.wm.Log.Std.Info("block scanner can not get new block data; unexpected error: %v", err)

		//记录未扫区块
		unscanRecord := NewUnscanRecord(height, "", err.Error())
		bs.SaveUnscanRecord(unscanRecord)
		bs.wm.Log.Std.Info("block height: %d extract failed.", height)
		return nil, err
	}

	bs.wm.Log.Std.Info("block scanner scanning height: %d ...", block.Height)

	err = bs.BatchExtractTransaction(block.Height, block.Hash, block.Transactions, false)
	if err != nil {
		bs.wm.Log.Std.Info("block scanner can not extractRechargeRecords; unexpected error: %v", err)
	}

	return block, nil
}

//ScanTxMemPool 扫描交易内存池
func (bs *BNBBlockScanner) ScanTxMemPool() {

	bs.wm.Log.Std.Info("block scanner scanning mempool ...")

	//提取未确认的交易单
	txIDsInMemPool, err := bs.wm.GetTxIDsInMemPool()
	if err != nil {
		bs.wm.Log.Std.Info("block scanner can not get mempool data; unexpected error: %v", err)
		return
	}

	if len(txIDsInMemPool) == 0 {
		bs.wm.Log.Std.Info("no transactions in mempool ...")
		return
	}

	err = bs.BatchExtractTransaction(0, "", txIDsInMemPool, true)
	if err != nil {
		bs.wm.Log.Std.Info("block scanner can not extractRechargeRecords; unexpected error: %v", err)
	}

}

//rescanFailedRecord 重扫失败记录
func (bs *BNBBlockScanner) RescanFailedRecord() {

	var (
		blockMap = make(map[uint64][]string)
	)

	list, err := bs.wm.GetUnscanRecords()
	if err != nil {
		bs.wm.Log.Std.Info("block scanner can not get rescan data; unexpected error: %v", err)
	}

	//组合成批处理
	for _, r := range list {

		if _, exist := blockMap[r.BlockHeight]; !exist {
			blockMap[r.BlockHeight] = make([]string, 0)
		}

		if len(r.TxID) > 0 {
			arr := blockMap[r.BlockHeight]
			arr = append(arr, r.TxID)

			blockMap[r.BlockHeight] = arr
		}
	}

	for height, txs := range blockMap {

		var hash string

		if height != 0 {
			bs.wm.Log.Std.Info("block scanner rescanning height: %d ...", height)

			if len(txs) == 0 {

				block, err := bs.wm.RpcClient.getBlockByHeight(height)
				if err != nil {
					bs.wm.Log.Std.Info("block scanner can not get new block data; unexpected error: %v", err)
					continue
				}

				txs = block.Transactions
			}

			err = bs.BatchExtractTransaction(height, hash, txs, false)
			if err != nil {
				bs.wm.Log.Std.Info("block scanner can not extractRechargeRecords; unexpected error: %v", err)
				continue
			}
		}
		//删除未扫记录
		bs.wm.DeleteUnscanRecord(height)
	}

	//删除未没有找到交易记录的重扫记录
	bs.wm.DeleteUnscanRecordNotFindTX()
}

//newBlockNotify 获得新区块后，通知给观测者
func (bs *BNBBlockScanner) newBlockNotify(block *Block, isFork bool) {
	header := block.BlockHeader()
	header.Fork = isFork
	bs.NewBlockNotify(header)
}

//BatchExtractTransaction 批量提取交易单
//bitcoin 1M的区块链可以容纳3000笔交易，批量多线程处理，速度更快
func (bs *BNBBlockScanner) BatchExtractTransaction(blockHeight uint64, blockHash string, txs []string, memPool bool) error {

	var (
		quit       = make(chan struct{})
		done       = 0 //完成标记
		failed     = 0
		shouldDone = len(txs) //需要完成的总数
	)

	if len(txs) == 0 {
		return nil
		// return errors.New("BatchExtractTransaction block is nil.")
	}

	//生产通道
	producer := make(chan ExtractResult)
	defer close(producer)

	//消费通道
	worker := make(chan ExtractResult)
	defer close(worker)

	//保存工作
	saveWork := func(height uint64, result chan ExtractResult) {
		//回收创建的地址
		for gets := range result {

			if gets.Success {

				notifyErr := bs.newExtractDataNotify(height, gets.extractData)
				//saveErr := bs.SaveRechargeToWalletDB(height, gets.Recharges)
				if notifyErr != nil {
					failed++ //标记保存失败数
					bs.wm.Log.Std.Info("newExtractDataNotify unexpected error: %v", notifyErr)
				}
			} else {
				//记录未扫区块
				unscanRecord := NewUnscanRecord(height, "", "")
				bs.SaveUnscanRecord(unscanRecord)
				bs.wm.Log.Std.Info("block height: %d extract failed.", height)
				failed++ //标记保存失败数
			}
			//累计完成的线程数
			done++
			if done == shouldDone {
				//bs.wm.Log.Std.Info("done = %d, shouldDone = %d ", done, len(txs))
				close(quit) //关闭通道，等于给通道传入nil
			}
		}
	}

	//提取工作
	extractWork := func(eblockHeight uint64, eBlockHash string, mTxs []string, eProducer chan ExtractResult) {
		for _, txid := range mTxs {
			bs.extractingCH <- struct{}{}
			//shouldDone++
			go func(mBlockHeight uint64, mTxid string, end chan struct{}, mProducer chan<- ExtractResult) {

				//导出提出的交易
				mProducer <- bs.ExtractTransaction(mBlockHeight, eBlockHash, mTxid, bs.ScanAddressFunc, memPool)
				//释放
				<-end

			}(eblockHeight, txid, bs.extractingCH, eProducer)
		}
	}

	/*	开启导出的线程	*/

	//独立线程运行消费
	go saveWork(blockHeight, worker)

	//独立线程运行生产
	go extractWork(blockHeight, blockHash, txs, producer)

	//以下使用生产消费模式
	bs.extractRuntime(producer, worker, quit)

	if failed > 0 {
		return fmt.Errorf("block scanner saveWork failed")
	} else {
		return nil
	}

	//return nil
}

//extractRuntime 提取运行时
func (bs *BNBBlockScanner) extractRuntime(producer chan ExtractResult, worker chan ExtractResult, quit chan struct{}) {

	var (
		values = make([]ExtractResult, 0)
	)

	for {
		var activeWorker chan<- ExtractResult
		var activeValue ExtractResult
		//当数据队列有数据时，释放顶部，传输给消费者
		if len(values) > 0 {
			activeWorker = worker
			activeValue = values[0]
		}
		select {
		//生成者不断生成数据，插入到数据队列尾部
		case pa := <-producer:
			values = append(values, pa)
		case <-quit:
			//退出
			//bs.wm.Log.Std.Info("block scanner have been scanned!")
			return
		case activeWorker <- activeValue:
			values = values[1:]
		}
	}
	//return

}

//ExtractTransaction 提取交易单
func (bs *BNBBlockScanner) ExtractTransaction(blockHeight uint64, blockHash string, txid string, scanAddressFunc openwallet.BlockScanAddressFunc, memPool bool) ExtractResult {

	var (
		result = ExtractResult{
			BlockHeight: blockHeight,
			TxID:        txid,
			extractData: make(map[string]*openwallet.TxExtractData),
			Success:     true,
		}
	)

	//bs.wm.Log.Std.Debug("block scanner scanning tx: %s ...", txid)
	var trx *Transaction
	var err error
	if memPool {
		trx, err = bs.wm.GetTransactionInMemPool(txid)
		if err != nil {
			trx, err = bs.wm.GetTransaction(txid)
			if err != nil {
				bs.wm.Log.Std.Info("block scanner can not extract transaction data in mempool and block chain; unexpected error: %v", err)
				result.Success = false
				return result
			}
		}
	} else {
		trx, err = bs.wm.GetTransaction(txid)

		if err != nil {
			bs.wm.Log.Std.Info("block scanner can not extract transaction data; unexpected error: %v", err)
			result.Success = false
			return result
		}
	}

	if trx != nil {
		//优先使用传入的高度
		if blockHeight > 0 && trx.BlockHeight == 0 {
			trx.BlockHeight = blockHeight
			///		trx.BlockHash = blockHash
		}
	}

	bs.extractTransaction(trx, &result, scanAddressFunc)

	return result

}

// 从最小单位的 amount 转为带小数点的表示
func convertToAmount(amount uint64, decimals uint64) string {
	amountStr := fmt.Sprintf("%d", amount)
	d, _ := decimal.NewFromString(amountStr)
	decimalStr := "1"
	for index := 0; index < int(decimals); index++ {
		decimalStr += "0"
	}
	w, _ := decimal.NewFromString(decimalStr)
	d = d.Div(w)
	return d.String()
}

// amount 字符串转为最小单位的表示
func convertFromAmount(amountStr string, decimals uint64) uint64 {
	d, _ := decimal.NewFromString(amountStr)
	decimalStr := "1"
	for index := 0; index < int(decimals); index++ {
		decimalStr += "0"
	}
	w, _ := decimal.NewFromString(decimalStr)
	d = d.Mul(w)
	r, _ := strconv.ParseInt(d.String(), 10, 64)
	return uint64(r)
}

//ExtractTransactionData 提取交易单
func (bs *BNBBlockScanner) extractTransaction(trx *Transaction, result *ExtractResult, scanAddressFunc openwallet.BlockScanAddressFunc) {
	var (
		success = true
	)
	createAt := time.Now().Unix()
	if trx == nil {
		//记录哪个区块哪个交易单没有完成扫描
		success = true
	} else {
		if success {
			feeSourceKey := ""
			blockhash, _ := bs.wm.RpcClient.getBlockHash(trx.BlockHeight)
			notifyFee := false
			feeNotified := false
			for _, detail := range trx.TxDetails {
				denom := detail.Denom
				for _, fromChk := range detail.From {
					sourceKey, ok := scanAddressFunc(fromChk.Address)
					if ok {
						feeSourceKey = sourceKey
						notifyFee = true

						var fromArray []string
						var toArray []string
						for i, from := range detail.From {

							input := openwallet.TxInput{}
							input.TxID = trx.TxID
							input.Address = from.Address
							input.Amount = strconv.FormatUint(from.Amount, 10)
							input.Coin = openwallet.Coin{
								Symbol:bs.wm.Symbol(),
								IsContract:true,
								ContractID:openwallet.GenContractID(bs.wm.Symbol(), denom),
								Contract:openwallet.SmartContract{
									Symbol:bs.wm.Symbol(),
									ContractID:openwallet.GenContractID(bs.wm.Symbol(), denom),
									Address:denom,
									Token:denom,
									Name:bs.wm.FullName(),
									Decimals:0,
								},
							}
							input.Index = uint64(i)
							input.Sid = openwallet.GenTxInputSID(trx.TxID, bs.wm.Symbol(), denom, input.Index)
							input.CreateAt = createAt
							input.BlockHeight = trx.BlockHeight
							input.BlockHash = blockhash
							input.IsMemo = true
							input.Memo = trx.Memo

							fromArray = append(fromArray, from.Address+":"+strconv.FormatUint(from.Amount, 10))

							ed := result.extractData[denom+":"+sourceKey]
							if ed == nil {
								ed = openwallet.NewBlockExtractData()
								result.extractData[denom+":"+sourceKey] = ed
							}
							ed.TxInputs = append(ed.TxInputs, &input)
						}

						for _, to := range detail.To {
							toArray = append(toArray, to.Address+":"+strconv.FormatUint(to.Amount, 10))
						}

						tx := &openwallet.Transaction{
							From:fromArray,
							To:toArray,
							Fees:"0",
							Coin:openwallet.Coin{
								Symbol:bs.wm.Symbol(),
								IsContract:true,
								ContractID:openwallet.GenContractID(bs.wm.Symbol(), denom),
								Contract:openwallet.SmartContract{
									Symbol:bs.wm.Symbol(),
									ContractID:openwallet.GenContractID(bs.wm.Symbol(), denom),
									Address:denom,
									Token:denom,
									Name:bs.wm.FullName(),
									Decimals:0,
								},
							},
							BlockHeight:trx.BlockHeight,
							BlockHash:blockhash,
							TxID:trx.TxID,
							Decimal:0,
							Status:"1",
							IsMemo:true,
							Memo:trx.Memo,
						}
						wxID := openwallet.GenTransactionWxID(tx)
						tx.WxID = wxID
						ed := result.extractData[denom+":"+sourceKey]
						if ed == nil {
							ed = openwallet.NewBlockExtractData()
							result.extractData[denom+":"+sourceKey] = ed
						}
						ed.Transaction = tx

						break
					}
				}

				if notifyFee && !feeNotified{
					var fee uint64
					var detail2  TxDetail
					if len(trx.TxDetails) > 1 {
						fee, _ = bs.wm.RpcClient.getMultiFeeByHeight(trx.BlockHeight)
					} else {
						for _, v := range trx.TxDetails {
							detail2  = *v
						}

						if len(detail2.From) > 1 || len(detail2.To) > 1 {
							fee, _ = bs.wm.RpcClient.getMultiFeeByHeight(trx.BlockHeight)
						} else {
							fee, _ = bs.wm.RpcClient.getFeeByHeight(trx.BlockHeight)
						}
					}

					feeCharge := openwallet.TxInput{}
					feeCharge.TxID = trx.TxID
					feeCharge.Address = detail.From[0].Address
					feeStr := strconv.FormatUint(fee, 10)
					feeCharge.Amount = feeStr
					feeCharge.Coin = openwallet.Coin{
						Symbol:bs.wm.Symbol(),
						IsContract:true,
						ContractID:openwallet.GenContractID(bs.wm.Symbol(), "BNB"),
						Contract:openwallet.SmartContract{
							Symbol:bs.wm.Symbol(),
							ContractID:openwallet.GenContractID(bs.wm.Symbol(), "BNB"),
							Address:"BNB",
							Token:"",
							Name:bs.wm.FullName(),
							Decimals:0,
						},
					}
					feeCharge.Index = 0
					feeCharge.Sid = openwallet.GenTxInputSID(trx.TxID, bs.wm.Symbol(), "BNB", feeCharge.Index)
					feeCharge.CreateAt = createAt
					feeCharge.BlockHeight = trx.BlockHeight
					feeCharge.BlockHash = blockhash
					feeCharge.IsMemo = true
					feeCharge.Memo = trx.Memo
					feeCharge.TxType = 1

					ed := result.extractData["fee:"+feeSourceKey]
					if ed == nil {
						ed = openwallet.NewBlockExtractData()
						result.extractData["fee:"+feeSourceKey] = ed
					}

					ed.TxInputs = append(ed.TxInputs, &feeCharge)

					tx := &openwallet.Transaction{
						From:[]string{detail.From[0].Address + ":" + feeStr},
						To:[]string{""},
						Amount:feeStr,
						Fees:"0",
						Coin:openwallet.Coin{
							Symbol:bs.wm.Symbol(),
							IsContract:true,
							ContractID:openwallet.GenContractID(bs.wm.Symbol(), "BNB"),
							Contract:openwallet.SmartContract{
								Symbol:bs.wm.Symbol(),
								ContractID:openwallet.GenContractID(bs.wm.Symbol(), "BNB"),
								Address:"BNB",
								Token:"",
								Name:bs.wm.FullName(),
								Decimals:0,
							},
						},
						BlockHash:blockhash,
						BlockHeight:trx.BlockHeight,
						TxID:trx.TxID,
						Decimal:0,
						Status:"1",
						IsMemo:true,
						Memo:trx.Memo,
						TxType:1,
					}
					wxID := openwallet.GenTransactionWxID(tx)
					tx.WxID = wxID
					ed.Transaction = tx

					feeNotified = true
				}

				for _, toChk := range detail.To {
					sourceKey, ok := scanAddressFunc(toChk.Address)
					if ok {
						var fromArray []string
						var toArray []string

						for i, to := range detail.To {
							output := openwallet.TxOutPut{}
							output.TxID = trx.TxID
							output.Address = to.Address
							output.Amount = strconv.FormatUint(to.Amount, 10)
							output.Coin = openwallet.Coin{
								Symbol:bs.wm.Symbol(),
								IsContract:true,
								ContractID:openwallet.GenContractID(bs.wm.Symbol(), denom),
								Contract:openwallet.SmartContract{
									Symbol:bs.wm.Symbol(),
									ContractID:openwallet.GenContractID(bs.wm.Symbol(), denom),
									Address:denom,
									Token:"",
									Name:bs.wm.FullName(),
									Decimals:0,
								},
							}
							output.Index = uint64(i)
							output.Sid = openwallet.GenTxOutPutSID(trx.TxID, bs.wm.Symbol(), denom, output.Index)
							output.CreateAt = createAt
							output.BlockHeight = trx.BlockHeight
							output.BlockHash = blockhash
							output.IsMemo = true
							output.Memo = trx.Memo

							ed := result.extractData[denom+":"+sourceKey]
							if ed == nil {
								ed = openwallet.NewBlockExtractData()
								result.extractData[denom+":"+sourceKey] = ed
							}
							ed.TxOutputs = append(ed.TxOutputs, &output)

							toArray = append(toArray, to.Address+":"+strconv.FormatUint(to.Amount, 10))
						}

						for _, from := range detail.From {
							fromArray = append(fromArray, from.Address+":"+strconv.FormatUint(from.Amount, 10))
						}

						ed := result.extractData[denom+":"+sourceKey]
						if ed == nil {
							ed = openwallet.NewBlockExtractData()
							result.extractData[denom+":"+feeSourceKey] = ed
						}
						if ed.Transaction == nil {
							tx := &openwallet.Transaction{
								From:fromArray,
								To:toArray,
								Fees:"0",
								Coin:openwallet.Coin{
									Symbol:bs.wm.Symbol(),
									IsContract:true,
									ContractID:openwallet.GenContractID(bs.wm.Symbol(), denom),
									Contract:openwallet.SmartContract{
										ContractID:openwallet.GenContractID(bs.wm.Symbol(), denom),
										Symbol:bs.wm.Symbol(),
										Address:denom,
										Token:"",
										Name:bs.wm.FullName(),
										Decimals:0,
									},
								},
								BlockHash:blockhash,
								BlockHeight:trx.BlockHeight,
								TxID:trx.TxID,
								Decimal:0,
								Status:"1",
								IsMemo:true,
								Memo:trx.Memo,
							}
							wxID := openwallet.GenTransactionWxID(tx)
							tx.WxID = wxID
							ed.Transaction = tx
						}
						result.Success = true
					}
					break
				}
			}
		}

		success = true

	}
	result.Success = success
}

//newExtractDataNotify 发送通知
func (bs *BNBBlockScanner) newExtractDataNotify(height uint64, extractData map[string]*openwallet.TxExtractData) error {

	for o, _ := range bs.Observers {
		for key, data := range extractData {
			key = strings.Split(key, ":")[1]
			err := o.BlockExtractDataNotify(key, data)
			if err != nil {
				bs.wm.Log.Error("BlockExtractDataNotify unexpected error:", err)
				//记录未扫区块
				unscanRecord := NewUnscanRecord(height, "", "ExtractData Notify failed.")
				err = bs.SaveUnscanRecord(unscanRecord)
				if err != nil {
					bs.wm.Log.Std.Error("block height: %d, save unscan record failed. unexpected error: %v", height, err.Error())
				}

			}
		}
	}

	return nil
}

//DeleteUnscanRecordNotFindTX 删除未没有找到交易记录的重扫记录
func (wm *WalletManager) DeleteUnscanRecordNotFindTX() error {

	//删除找不到交易单
	reason := "[-5]No information available about transaction"

	//获取本地区块高度
	db, err := storm.Open(filepath.Join(wm.Config.dbPath, wm.Config.BlockchainFile))
	if err != nil {
		return err
	}
	defer db.Close()

	var list []*UnscanRecord
	err = db.All(&list)
	if err != nil {
		return err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	for _, r := range list {
		if strings.HasPrefix(r.Reason, reason) {
			tx.DeleteStruct(r)
		}
	}
	return tx.Commit()
}

//SaveRechargeToWalletDB 保存交易单内的充值记录到钱包数据库
//func (bs *BNBBlockScanner) SaveRechargeToWalletDB(height uint64, list []*openwallet.Recharge) error {
//
//	for _, r := range list {
//
//		//accountID := "W4ruoAyS5HdBMrEeeHQTBxo4XtaAixheXQ"
//		wallet, ok := bs.GetWalletByAddress(r.Address)
//		if ok {
//
//			//a := wallet.GetAddress(r.Address)
//			//if a == nil {
//			//	continue
//			//}
//			//
//			//r.AccountID = a.AccountID
//
//			err := wallet.SaveUnreceivedRecharge(r)
//			//如果blockHash没有值，添加到重扫，避免遗留
//			if err != nil || len(r.BlockHash) == 0 {
//
//				//记录未扫区块
//				unscanRecord := NewUnscanRecord(height, r.TxID, "save to wallet failed.")
//				err = bs.SaveUnscanRecord(unscanRecord)
//				if err != nil {
//					bs.wm.Log.Std.Error("block height: %d, txID: %s save unscan record failed. unexpected error: %v", height, r.TxID, err.Error())
//				}
//
//			} else {
//				bs.wm.Log.Info("block scanner save blockHeight:", height, "txid:", r.TxID, "address:", r.Address, "successfully.")
//			}
//		} else {
//			return errors.New("address in wallet is not found")
//		}
//
//	}
//
//	return nil
//}

//GetCurrentBlockHeader 获取全网最新高度区块头
func (bs *BNBBlockScanner) GetCurrentBlockHeader() (*openwallet.BlockHeader, error) {
	var (
		blockHeight uint64 = 0
		err         error
	)

	blockHeight, err = bs.wm.GetBlockHeight()
	if err != nil {
		return nil, err
	}

	block, err := bs.wm.RpcClient.getBlockByHeight(blockHeight)
	if err != nil {
		bs.wm.Log.Errorf("get block spec by block number failed, err=%v", err)
		return nil, err
	}

	return &openwallet.BlockHeader{Height: blockHeight, Hash: block.Hash}, nil
}

//GetScannedBlockHeader 获取已扫高度区块头
func (bs *BNBBlockScanner) GetScannedBlockHeader() (*openwallet.BlockHeader, error) {

	var (
		blockHeight uint64 = 0
		hash        string
		err         error
	)

	blockHeight, hash, err = bs.wm.GetLocalNewBlock()
	if err != nil {
		bs.wm.Log.Errorf("get local new block failed, err=%v", err)
		return nil, err
	}

	//如果本地没有记录，查询接口的高度
	if blockHeight == 0 {
		blockHeight, err = bs.wm.GetBlockHeight()
		if err != nil {
			bs.wm.Log.Errorf("BNB GetBlockHeight failed,err = %v", err)
			return nil, err
		}

		//就上一个区块链为当前区块
		blockHeight = blockHeight - 1

		block, err := bs.wm.RpcClient.getBlockByHeight(blockHeight)
		if err != nil {
			bs.wm.Log.Errorf("get block spec by block number failed, err=%v", err)
			return nil, err
		}

		hash = block.Hash
	}

	return &openwallet.BlockHeader{Height: blockHeight, Hash: hash}, nil
}

//GetScannedBlockHeight 获取已扫区块高度
func (bs *BNBBlockScanner) GetScannedBlockHeight() uint64 {
	localHeight, _, _ := bs.wm.GetLocalNewBlock()
	return localHeight
}

func (bs *BNBBlockScanner) ExtractTransactionData(txid string, scanTargetFunc openwallet.BlockScanTargetFunc) (map[string][]*openwallet.TxExtractData, error) {

	scanAddressFunc := func(address string) (string, bool) {
		target := openwallet.ScanTarget{
			Address:          address,
			BalanceModelType: openwallet.BalanceModelTypeAddress,
		}
		return scanTargetFunc(target)
	}
	result := bs.ExtractTransaction(0, "", txid, scanAddressFunc, false)
	if !result.Success {
		return nil, fmt.Errorf("extract transaction failed")
	}
	extData := make(map[string][]*openwallet.TxExtractData)
	for key, data := range result.extractData {
		txs := extData[key]
		if txs == nil {
			txs = make([]*openwallet.TxExtractData, 0)
		}
		txs = append(txs, data)
		extData[key] = txs
	}
	return extData, nil
}

//DropRechargeRecords 清楚钱包的全部充值记录
//func (bs *BNBBlockScanner) DropRechargeRecords(accountID string) error {
//	bs.mu.RLock()
//	defer bs.mu.RUnlock()
//
//	wallet, ok := bs.walletInScanning[accountID]
//	if !ok {
//		errMsg := fmt.Sprintf("accountID: %s wallet is not found", accountID)
//		return errors.New(errMsg)
//	}
//
//	return wallet.DropRecharge()
//}

//DeleteRechargesByHeight 删除某区块高度的充值记录
//func (bs *BNBBlockScanner) DeleteRechargesByHeight(height uint64) error {
//
//	bs.mu.RLock()
//	defer bs.mu.RUnlock()
//
//	for _, wallet := range bs.walletInScanning {
//
//		list, err := wallet.GetRecharges(false, height)
//		if err != nil {
//			return err
//		}
//
//		db, err := wallet.OpenDB()
//		if err != nil {
//			return err
//		}
//
//		tx, err := db.Begin(true)
//		if err != nil {
//			return err
//		}
//
//		for _, r := range list {
//			err = db.DeleteStruct(&r)
//			if err != nil {
//				return err
//			}
//		}
//
//		tx.Commit()
//
//		db.Close()
//	}
//
//	return nil
//}

//SaveTxToWalletDB 保存交易记录到钱包数据库
func (bs *BNBBlockScanner) SaveUnscanRecord(record *UnscanRecord) error {

	if record == nil {
		return errors.New("the unscan record to save is nil")
	}

	//if record.BlockHeight == 0 {
	//	return errors.New("unconfirmed transaction do not rescan")
	//}

	//获取本地区块高度
	db, err := storm.Open(filepath.Join(bs.wm.Config.dbPath, bs.wm.Config.BlockchainFile))
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Save(record)
}

//GetSourceKeyByAddress 获取地址对应的数据源标识
func (bs *BNBBlockScanner) GetSourceKeyByAddress(address string) (string, bool) {
	bs.Mu.RLock()
	defer bs.Mu.RUnlock()

	sourceKey, ok := bs.AddressInScanning[address]
	return sourceKey, ok
}

//GetWalletByAddress 获取地址对应的钱包
// func (bs *BNBBlockScanner) GetWalletByAddress(address string) (*openwallet.Wallet, bool) {
// 	bs.mu.RLock()
// 	defer bs.mu.RUnlock()

// 	account, ok := bs.addressInScanning[address]
// 	if ok {
// 		wallet, ok := bs.walletInScanning[account]
// 		return wallet, ok

// 	} else {
// 		return nil, false
// 	}
// }

//GetBlockHeight 获取区块链高度
func (wm *WalletManager) GetBlockHeight() (uint64, error) {
	return wm.RpcClient.getBlockHeight()
}

//GetLocalNewBlock 获取本地记录的区块高度和hash
func (wm *WalletManager) GetLocalNewBlock() (uint64, string, error) {

	var (
		blockHeight uint64 = 0
		blockHash   string = ""
	)

	//获取本地区块高度
	db, err := storm.Open(filepath.Join(wm.Config.dbPath, wm.Config.BlockchainFile))
	if err != nil {
		return 0, "", err
	}
	defer db.Close()

	err = db.Get(blockchainBucket, "blockHeight", &blockHeight)
	if err != nil && err != storm.ErrNotFound {
		wm.Log.Errorf("get local block height failed, err = %v", err)
		return 0, "", err
	}

	err = db.Get(blockchainBucket, "blockHash", &blockHash)
	if err != nil && err != storm.ErrNotFound {
		wm.Log.Errorf("get local block hash failed, err = %v", err)
		return 0, "", err
	}
	return blockHeight, blockHash, nil
}

//SaveLocalNewBlock 记录区块高度和hash到本地
func (wm *WalletManager) SaveLocalNewBlock(blockHeight uint64, blockHash string) {

	//获取本地区块高度
	db, err := storm.Open(filepath.Join(wm.Config.dbPath, wm.Config.BlockchainFile))
	if err != nil {
		return
	}
	defer db.Close()

	db.Set(blockchainBucket, "blockHeight", &blockHeight)
	db.Set(blockchainBucket, "blockHash", &blockHash)
}

//SaveLocalBlock 记录本地新区块
func (wm *WalletManager) SaveLocalBlock(block *Block) {

	db, err := storm.Open(filepath.Join(wm.Config.dbPath, wm.Config.BlockchainFile))
	if err != nil {
		return
	}
	defer db.Close()

	db.Save(block)
}

//GetBlockHash 根据区块高度获得区块hash
func (wm *WalletManager) GetBlockHash(height uint64) (string, error) {
	return wm.RpcClient.getBlockHash(height)
}

//GetLocalBlock 获取本地区块数据
func (wm *WalletManager) GetLocalBlock(height uint64) (*Block, error) {

	var (
		block Block
	)

	db, err := storm.Open(filepath.Join(wm.Config.dbPath, wm.Config.BlockchainFile))
	if err != nil {
		return nil, err
	}
	defer db.Close()

	err = db.One("Height", height, &block)
	if err != nil {
		return nil, err
	}

	return &block, nil
}

//GetBlock 获取区块数据
func (wm *WalletManager) GetBlock(hash string) (*Block, error) {
	return nil, errors.New("get block by hash is not supported right now!")
}

//GetTxIDsInMemPool 获取待处理的交易池中的交易单IDs
func (wm *WalletManager) GetTxIDsInMemPool() ([]string, error) {
	return nil, nil

}

func (wm *WalletManager) GetTransactionInMemPool(txid string) (*Transaction, error) {
	return nil, nil
}

//GetTransaction 获取交易单
func (wm *WalletManager) GetTransaction(txid string) (*Transaction, error) {
	return wm.RpcClient.getTransaction(txid)
}

//获取未扫记录
func (wm *WalletManager) GetUnscanRecords() ([]*UnscanRecord, error) {
	//获取本地区块高度
	db, err := storm.Open(filepath.Join(wm.Config.dbPath, wm.Config.BlockchainFile))
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var list []*UnscanRecord
	err = db.All(&list)
	if err != nil {
		return nil, err
	}
	return list, nil
}

//DeleteUnscanRecord 删除指定高度的未扫记录
func (wm *WalletManager) DeleteUnscanRecord(height uint64) error {
	//获取本地区块高度
	db, err := storm.Open(filepath.Join(wm.Config.dbPath, wm.Config.BlockchainFile))
	if err != nil {
		return err
	}
	defer db.Close()

	var list []*UnscanRecord
	err = db.Find("BlockHeight", height, &list)
	if err != nil {
		return err
	}

	for _, r := range list {
		db.DeleteStruct(r)
	}

	return nil
}

//GetAssetsAccountBalanceByAddress 查询账户相关地址的交易记录
func (bs *BNBBlockScanner) GetBalanceByAddress(address ...string) ([]*openwallet.Balance, error) {

	addrsBalance := make([]*openwallet.Balance, 0)

	for _, addr := range address {
		balance, err := bs.wm.RpcClient.getBalance(addr, "BNB")
		if err != nil {
			return nil, err
		}

		addrsBalance = append(addrsBalance, &openwallet.Balance{
			Symbol:  bs.wm.Symbol(),
			Address: addr,
			Balance: convertToAmount(uint64(balance.Balance.Int64()), 8),
		})
	}

	return addrsBalance, nil
}

func (c *Client) getMultiAddrTransactions(offset, limit int, addresses ...string) ([]*Transaction, error) {
	var (
		trxs = make([]*Transaction, 0)
	)

	for _, addr := range addresses {
		path := "/txs/" + addr

		resp, err := c.Call(path, nil, "GET")
		if err != nil {
			return nil, err
		}
		txArray := resp.Array()[0].Array()

		for _, txDetail := range txArray {
			trxs = append(trxs, NewTransaction(&txDetail))
		}
	}

	return trxs, nil
}

//GetAssetsAccountTransactionsByAddress 查询账户相关地址的交易记录
func (bs *BNBBlockScanner) GetTransactionsByAddress(offset, limit int, coin openwallet.Coin, address ...string) ([]*openwallet.TxExtractData, error) {

	var (
		array = make([]*openwallet.TxExtractData, 0)
	)

	trxs, err := bs.wm.RpcClient.getMultiAddrTransactions(offset, limit, address...)
	if err != nil {
		return nil, err
	}

	key := "account"

	//提取账户相关的交易单
	var scanAddressFunc openwallet.BlockScanAddressFunc = func(findAddr string) (string, bool) {
		for _, a := range address {
			if findAddr == a {
				return key, true
			}
		}
		return "", false
	}

	//要检查一下tx.BlockHeight是否有值

	for _, tx := range trxs {

		result := ExtractResult{
			BlockHeight: tx.BlockHeight,
			TxID:        tx.TxID,
			extractData: make(map[string]*openwallet.TxExtractData),
			Success:     true,
		}

		bs.extractTransaction(tx, &result, scanAddressFunc)
		data := result.extractData
		txExtract := data[key]
		if txExtract != nil {
			array = append(array, txExtract)
		}

	}

	return array, nil
}

//Run 运行
func (bs *BNBBlockScanner) Run() error {

	bs.BlockScannerBase.Run()

	return nil
}

////Stop 停止扫描
func (bs *BNBBlockScanner) Stop() error {

	bs.BlockScannerBase.Stop()

	return nil
}

//Pause 暂停扫描
func (bs *BNBBlockScanner) Pause() error {

	bs.BlockScannerBase.Pause()

	return nil
}

//Restart 继续扫描
func (bs *BNBBlockScanner) Restart() error {

	bs.BlockScannerBase.Restart()

	return nil
}

/******************* 使用insight socket.io 监听区块 *******************/

//setupSocketIO 配置socketIO监听新区块
func (bs *BNBBlockScanner) setupSocketIO() error {

	bs.wm.Log.Info("block scanner use socketIO to listen new data")

	var (
		room = "inv"
	)

	if bs.socketIO == nil {

		apiUrl, err := url.Parse(bs.wm.Config.RpcAPI)
		if err != nil {
			return err
		}
		domain := apiUrl.Hostname()
		port := common.NewString(apiUrl.Port()).Int()
		c, err := gosocketio.Dial(
			gosocketio.GetUrl(domain, port, false),
			transport.GetDefaultWebsocketTransport())
		if err != nil {
			return err
		}

		bs.socketIO = c

	}

	err := bs.socketIO.On("tx", func(h *gosocketio.Channel, args interface{}) {
		//bs.wm.Log.Info("block scanner socketIO get new transaction received: ", args)
		txMap, ok := args.(map[string]interface{})
		if ok {
			txid := txMap["txid"].(string)
			errInner := bs.BatchExtractTransaction(0, "", []string{txid}, false)
			if errInner != nil {
				bs.wm.Log.Std.Info("block scanner can not extractRechargeRecords; unexpected error: %v", errInner)
			}
		}

	})
	if err != nil {
		return err
	}

	/*
		err = bs.socketIO.On("block", func(h *gosocketio.Channel, args interface{}) {
			bs.wm.Log.Info("block scanner socketIO get new block received: ", args)
			hash, ok := args.(string)
			if ok {

				block, errInner := bs.wm.GetBlock(hash)
				if errInner != nil {
					bs.wm.Log.Std.Info("block scanner can not get new block data; unexpected error: %v", errInner)
				}

				errInner = bs.scanBlock(block)
				if errInner != nil {
					bs.wm.Log.Std.Info("block scanner can not block: %d; unexpected error: %v", block.Height, errInner)
				}
			}

		})
		if err != nil {
			return err
		}
	*/

	err = bs.socketIO.On(gosocketio.OnDisconnection, func(h *gosocketio.Channel) {
		bs.wm.Log.Info("block scanner socketIO disconnected")
	})
	if err != nil {
		return err
	}

	err = bs.socketIO.On(gosocketio.OnConnection, func(h *gosocketio.Channel) {
		bs.wm.Log.Info("block scanner socketIO connected")
		h.Emit("subscribe", room)
	})
	if err != nil {
		return err
	}

	return nil
}
