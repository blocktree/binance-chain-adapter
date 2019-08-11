package binancechain

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

const(
	nodeurl = ""
)


func Test_getBlockHeight(t *testing.T) {
	c := NewClient(nodeurl, false)

	r, err := c.getBlockHeight()

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(r)
	}
}

func Test_getBlockByHash(t *testing.T) {
	hash := "3Uvb87ukKKwVeU6BFsZ21hy9sSbSd3Rd5QZTWbNop1d3TaY9ZzceJAT54vuY8XXQmw6nDx8ZViPV3cVznAHTtiVE"

	c := NewClient(nodeurl, false)

	r, err := c.Call("blocks/signature/"+hash, nil, "GET")

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(r)
	}
}

func Test_getBlockHash(t *testing.T) {
	c := NewClient(nodeurl, false)

	height := uint64(20552705)

	r, err := c.getBlockHash(height)

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(r)
	}

}
func Test_tmp(t *testing.T) {
	test, err := time.Parse(time.RFC3339Nano, "2019-05-08T02:13:41.937681458Z")
	fmt.Println(err)
	fmt.Println(test.Unix())
}
func Test_getBalance(t *testing.T) {
	c := NewClient(nodeurl, false)

	address := "bnb1mg0jn2nfcueszxv0g60tlhurj7j96hlc8en9lu"

	r, err := c.getBalance(address, "BNB")

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(r)
	}

}

func Test_getTransaction(t *testing.T) {
	c := NewClient(nodeurl, false)
	txid := "AA1F7401C18E90A8D3AE54EFB69BF630F6543A471D26917B23DE2E7ECB730C9A" //"9KBoALfTjvZLJ6CAuJCGyzRA1aWduiNFMvbqTchfBVpF"
	//txid := "E56F4DE446C0A6248B2F718A98FA29702ACBBC1F19050D1ABDD392A845C279FE"
	r, err := c.getTransaction(txid)

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(r)
	}

	//trx := NewTransaction(r, "auth/StdTx", "cosmos-sdk/MsgSend", "muon")

	//fmt.Println(trx)
}

func Test_getFee(t *testing.T) {
	c := NewClient(nodeurl, false)
	height := uint64(25606408)

	resp, err := c.getFeeByHeight(height)

	fmt.Println(err)
	fmt.Println(resp)
}

func Test_convert(t *testing.T) {

	amount := uint64(5000000001)

	amountStr := fmt.Sprintf("%d", amount)

	fmt.Println(amountStr)

	d, _ := decimal.NewFromString(amountStr)

	w, _ := decimal.NewFromString("100000000")

	d = d.Div(w)

	fmt.Println(d.String())

	d = d.Mul(w)

	fmt.Println(d.String())

	r, _ := strconv.ParseInt(d.String(), 10, 64)

	fmt.Println(r)

	fmt.Println(time.Now().UnixNano())
}

func Test_getTransactionByAddresses(t *testing.T) {
	addrs := "ARAA8AnUYa4kWwWkiZTTyztG5C6S9MFTx11"

	c := NewClient(nodeurl, false)
	result, err := c.getMultiAddrTransactions( 0, -1, addrs)

	if err != nil {
		t.Error("get transactions failed!")
	} else {
		for _, tx := range result {
			fmt.Println(tx.TxID)
		}
	}
}

func Test_getBlockByHeight(t *testing.T) {
	height := uint64(25601756)
	c := NewClient(nodeurl, false)
	result, err := c.getBlockByHeight(height)
	if err != nil {
		t.Error("get block failed!")
	} else {
		fmt.Println(result)
	}
}

func Test_sequence(t *testing.T) {
	addr := "bnb143f9qp4xe2fsh60rdsj8wx47zcmkununuwmp4l"
	c := NewClient(nodeurl, false)
	accountnumber, sequence, err := c.getAccountNumberAndSequence(addr)
	fmt.Println(err)
	fmt.Println(accountnumber)
	fmt.Println(sequence)
}

func Test_send(t *testing.T) {
	tx := "c401f0625dee0a4c2a2c87fa0a220a14ac525006a6ca930be9e36c24771abe16376e4f93120a0a03424e4210c096b10212220a1433b9e9c387328b16823aa9a0dbfa22c4dcacd80a120a0a03424e4210c096b102126e0a26eb5ae9872103b523b7507d11848a2a8df49907a22d39f473887a8578370830cc48e42b44859d1240c76a6f0d0b778a025f64a270a7edb9fdd02dd52b6a4286ec26de4b68851106cb3fe08adaebb6081a0a15ff002986ce217db45592bf548853eb69794ed0bafb6918f4df0c2001"
	c := NewClient(nodeurl, false)

	txid, err := c.sendTransaction(tx)
	fmt.Println(err)
	fmt.Println(txid)
}