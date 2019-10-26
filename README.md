# flink-in-action
实用的flink使用的范例：

### simple-actions下是一些简单的使用范例,包括:
(1) batch目录下是一个简单的flink批处理应用--batch wordcount.

(2) dataproduct目录下是一个模拟生成kafka消息的工具.

(3) rdbms目录下是一个消费kafka消息并将结果写入mysql的flink流式计算应用.

(4) streaming目录下是一个简单的flink流式计算应用--straming wordcount.

(5).streamingsql目录下是一个flink streaming sql应用，基于flink 1.9.0版本，可以实时从kafka接收数据并经过简单的sql etl，将结果
写入mysql表中.

(6)licenseNumber目录下是一个车牌号限制汇总系统,它通过消费kafka中采集到的车辆监控信息,与限号规则进行比较,来判定车辆是否违规.


### recommand-actions结合web-actions是一个基于Flink实现的商品实时推荐系统，它基于实时日志对用户进行画像，并根据画像结果将热门商品排序并推荐给用户(目前仍在完善中)
其共分为6个子任务:

(1)日志任务:用户访问商品日志数据写入kafka,然后flink任务消费kafka的日志topic,不做过滤直接写入hbase中

(2)浏览历史记录任务:读取用户访问商品日志记录,并将用户访问表的对应用户的商品访问加1,将商品访问表的对应商品的用户的访问加1

(3)用户兴趣任务:读取用户访问商品日志记录,并记录用户在指定的间隔时间内(100s),连续发生的操作行为,并据此提高用户对某商品的兴趣度

(4)用户画像任务:读取用户访问商品日志记录,并根据商品的产地颜色风格三维特征,记录用户对这些特征的喜好程度,进行用户兴趣画像

(5)商品画像任务:读取用户访问商品日志记录,并根据浏览商品的用户的性别及年龄特征,记录产品受这些特征用户的喜好程度,进行产品画像

(6)热门商品任务:读取用户访问商品日志记录,通过ListState存储热度商品,每5秒输出一次最近60秒的商品热度情况


recommand-actions中的scheduler包下:

(1)ItemCfCoeff实现了协同过滤算法策略,计算产品的相关度,主要依据是浏览该产品的用户的相似度

(2)ProductCoeff实现了余弦相似度算法策略, 计算产品相关度



另外，还有一些工具方法，包括simple-actions目录下：

source-generator.sh用来生成kafka topic的消息

env.sh下是flink和kafka的安装目录，需要根据具体目录调整

示例1-3的建表语句如下：

CREATE TABLE `pvuv_sink` (
  `dt` varchar(128) NOT NULL DEFAULT '0',
  `pv` bigint(64) DEFAULT NULL,
  `uv` bigint(64) DEFAULT NULL,
  PRIMARY KEY (`dt`),
  UNIQUE KEY `dt_UNIQUE` (`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
