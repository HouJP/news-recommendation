****

##<center>新华社推荐系统</center>
####<center>E-mail: houjp1992@gmail.com</center>

****

###目录
*	[项目介绍](#intro)
*	[使用说明](#usage)
*	[数据说明](#data)
*	[版本更新](#version)

****

###<a name="intro">项目介绍</a>

基于Spark、Spray提供新闻推荐服务（针对用户及用户当前正在浏览的新闻）。

*	增量计算模块：每日三点启动离线计算模块，根据HDFS上的新增数据（历史新闻和用户浏览记录），增量更新词表及用户向量，并生成新的推荐模型。
*	更新候选文档集接口：共20份候选文档集，每份2000条最新新闻，约每三小时新增2000条最新新闻，替换距离当前时间最远的2000条旧新闻，保证可以将最新新闻推荐给用户。
*	新闻推荐接口：根据用户当前正在浏览的新闻，以及用户历史浏览记录，调用推荐模型从候选文档集中推荐topK条最新新闻。
*	日志及中间计算结果保存：滚动窗口的形式保留一周内的更新状态，按日期存储，用于出错回滚，快速恢复到正常状态。

框架如下 ![frame](https://github.com/houjp/NewsRecommendation/raw/master/img/frame.jpg)

****

###<a name="usage">使用说明</a>

*	./boot.sh init <date> <data_dir>
	*	说明：初始化环境
	* 	parameter <date>: 初始化后所在的日期
	*	parameter <data_dir>: 数据存放的根目录

*	./boot.sh offline <pre_date> <date> <data_dir>
	*	说明：更新数据集
	*	parameter <pre_date>: 历史日期
	*	parameter <date>: 新增数据日期
	*	parameter <date_dir>: 数据存放的根目录

*	./boot.sh online <date> <data_dir> <host> <port>
	* 	说明：启动在线服务
	*	parameter <date>: 新增数据日期
	*	parameter <data_dir>: 数据存放的根目录
	*	parameter <host>: 服务IP
	*	parameter <port>: 服务端口
	
*	./merge.sh
	*	说明：用来添加到crontab
		*	根据当前日期执行增量式的离线计算，并重启在线服务
		*	记录日志到 log/

*	`新闻推荐`查询请求：

```
curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d '{"uid":"1000000142","doc_id":"01793009222687981568","content":"我们 交通","count":3}' "http://10.100.1.50:8124/golaxy/news/recommend"

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d '{"uid":"1000000142","doc_id":"01793009222687981568","content":"我们 交通","count":3}' "http://10.100.1.50:8124/golaxy/oversea_news/recommend"
```

*	`新闻推荐`更新请求：

```
// 注意两条命令的url是不同的

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d '{"dir":"/user/lihb/news_doc","filename":"candidate_news_0.txt"}' "http://10.100.1.50:8124/golaxy/news/update"

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d '{"dir":"/user/lihb/oversea_news_doc","filename":"candidate_news_0.txt"}' "http://10.100.1.50:8124/golaxy/oversea_news/update"
```

* 	`新闻推荐`停止服务：

```
curl -X POST "http://10.100.1.50:8124/stop"
```

*	`关键词推荐`查询请求：

```
curl -X POST -H "Content-Type:application/json" -H "Accept:application/json" -d '{"words":"李世石"}' "http://10.100.1.50:8488/golaxy/recommend/key-words"
```

*	`事件推荐`查询请求：

```
curl -X POST -H "Content-Type:application/json" -H "Accept:application/json" -d '{"uid":"132531345"}' "http://10.100.1.50:8488/golaxy/recommend/events"
```

*	`关键词推荐`和`事件推荐`停止服务：

```
curl -X POST "http://10.100.1.50:8488/golaxy/recommend/stop"
```

****

###<a name="data">数据说明</a>

*	用户点击信息
	*	路径：data/user_info
	*	说明：数据格式从左至右分别是，用户id，新闻id，用户点击时间。字段之间使用tab键隔开。
	
*	浏览的新闻文档
	*	路径：data/docs
	*	说明：用户前一天浏览的新闻信息，json格式。

*	候选集
	*	路径：data/news_doc
	*	说明：候选新闻，json格式
	
*	离线处理结果文件
	*	路径：data/cache/

****

###<a name="version">版本更新</a>

* 	2016/03/16
	*	重新设计程序框架
	*	添加`关键词推荐`功能
	*	添加`事件推荐`功能

*	2015/08/26
	*	添加`bin/merge.sh`
	*	添加`bin/boot.sh`
	*	重写离线计算（增量式更新）和在线推荐模块
	*	增加候选文档集更新功能

*	2015/08/18
	*	离线在线处理的分词组件改用Ansj。
	*	返回的结果根据JaccardSimilarity进行去重。

*	2015/08/17
	*	限制用户向量长度
	*	对向量进行归一化

*	2015/08/12
	*	更改用户向量计算方式

*	2015/08/11
	*	优化tf-idf计算
	*	代码整理，去除冗余

*	2015/08/10
	*	离线处理部分完成
		*	历史新闻文档向量计算
		*	候选新闻文档向量计算
		*	用户向量计算
			*	根据数据集提取关键词构造用户向量
			*	根据tf-idf排序提取关键词构造用户向量
	*	在线处理部分完成
		*	计算context_vector
		*	计算context_vector与候选新闻文档向量相似度

****
