--示例1-3的建表语句如下：

CREATE TABLE `pvuv_sink` (
  `dt` varchar(128) NOT NULL DEFAULT '0',
  `pv` bigint(64) DEFAULT NULL,
  `uv` bigint(64) DEFAULT NULL,
  PRIMARY KEY (`dt`),
  UNIQUE KEY `dt_UNIQUE` (`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;