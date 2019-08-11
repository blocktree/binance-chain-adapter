# binance-chain-adapter

本项目适配了openwallet.AssetsAdapter接口，给应用提供了底层的区块链协议支持。

## 如何测试

openwtester包下的测试用例已经集成了openwallet钱包体系，创建conf文件，新建BNB.ini文件，编辑如下内容：

```ini
# mainnet rest api url
rpcAPI = "http://47.244.179.69:20012"

# Cache data file directory, default = "", current directory: ./data
dataDir = ""
```
