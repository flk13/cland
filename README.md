# crenter
Crenter is a scheduler center for distributed crawlers with Zookeeper and HDFS.

更多设计方法和说明请查看[博客](https://mapleuz.cc/project-crenter)

## 依赖

- ZooKeeper
- HDFS
- JDK8

## 环境

该项目的设计是为了分布式，所以最好在集群上运行，当然，单机测试也是支持的。

- 首先需要将该项目根目录加入环境变量，命名为 `CLAND_HOME`

- 启动集群的ZooKeeper和HDFS服务

- 定制配置文件

`zkserver`中需要写入你的zookeeper集群的connect string，每个connect string单独成一行

需要按照需求修改bloomfiter的bloom_filter_enums（最大容量）和bloom_filter_fpr（误差概率）这两个属性

## 运行

- 使用/bin下的cdformat脚本，初始化zookeeper和hdfs的目录树

```
$ /bin/cdformat -n
```

- 启动manager服务，第一台机器启动的manager为`active`状态，后续启动的为`standby`状态

```
$ /bin/cland -r manager
```

- 启动worker服务，爬虫节点需要启动，如果需要shell功能也需要开启此服务

```
$ /bin/cland -r worker
```

## 配置爬虫

爬虫端可自定义，具体实现请看`src/python`下的示例。
