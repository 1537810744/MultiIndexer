删除之前的多链监听索引器，业务场景不合理，我们变更项目为：

《高并发区块链浏览器》

参考github上标准规范写法。

（比如：https://etherscan.io/  https://blockchair.com/zh/bitcoin https://github.com/GMWalletApp/epusdt 当然这是我随便找的）

写一个简单的区块链浏览器（三条链即可eth btc sol）

顶部是查询（输入address、交易哈希等等等可以查询）

switch面板，btc，eth，sol 切换

曲线：该链的原生代币价格曲线

表格：该链从新到旧的块

点击块号，进去可以看到当前块内打包的交易。

工具：golang，docker（nginx redis kafka sql 等等等） 适当拆分为微服务。 

我想的是先从区块链读到本地数据库，然后对外提供restful api 展示

![1、searchbar](C:\Users\admin\Desktop\MultiExplore\1、searchbar.png)

![someBlock's native coin price](C:\Users\admin\Desktop\MultiExplore\someBlock's native coin price.png)

![2 blocklist](C:\Users\admin\Desktop\MultiExplore\2 blocklist.png)

![someBlock's transaction list](C:\Users\admin\Desktop\MultiExplore\someBlock's transaction list.png)