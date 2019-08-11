package binancechain

import (
	"fmt"
	"github.com/blocktree/openwallet/log"
	"github.com/blocktree/openwallet/openwallet"
	"github.com/shopspring/decimal"
	"math/big"
)

type AddrBalance struct {
	Address string
	Balance *big.Int
	index   int
}

func convertToAmountWithDecimal(amount, decimals uint64) string {
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

type ContractDecoder struct {
	*openwallet.SmartContractDecoderBase
	wm *WalletManager
}

//NewContractDecoder 智能合约解析器
func NewContractDecoder(wm *WalletManager) *ContractDecoder {
	decoder := ContractDecoder{}
	decoder.wm = wm
	return &decoder
}

func (decoder *ContractDecoder) GetTokenBalanceByAddress(contract openwallet.SmartContract, address ...string) ([]*openwallet.TokenBalance, error) {

	 var tokenBalanceList []*openwallet.TokenBalance

	for i := 0; i < len(address); i++ {
		tokenBalance := openwallet.TokenBalance{
			Contract: &contract,
		}

		balance, err := decoder.wm.RpcClient.getBalance(address[i], contract.Address)
		if err != nil {
			log.Error("Get ONT balance of address [%v] failed with error : [%v]", address[i], err)
			return nil, err
		}
		tokenBalance.Balance = &openwallet.Balance{
			Address:          address[i],
			Symbol:           contract.Symbol,
			Balance:          convertToAmountWithDecimal(balance.Balance.Uint64(), contract.Decimals),
			ConfirmBalance:   convertToAmountWithDecimal(balance.Balance.Uint64(), contract.Decimals),
			UnconfirmBalance: "0",
		}

		tokenBalanceList = append(tokenBalanceList, &tokenBalance)
	}

	return tokenBalanceList, nil
}
