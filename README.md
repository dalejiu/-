# 基于大数据开发的计算机岗位数据分析系统

这是一个基于大数据技术开发的计算机岗位数据分析系统，旨在通过分布式计算和数据库技术分析招聘数据。本文档提供了代码平台搭建指南、大数据服务的启动与停止命令，以及服务器数据库配置说明。

## 项目概述
- **名称**：计算机岗位数据分析系统  
- **技术栈**：Hadoop、Yarn、Hive、Spark、Zookeeper、MySQL  
- **功能**：分析招聘数据，提供可视化结果和推荐功能  

## 代码平台搭建

### 虚拟机IP
  node1: 192.168.88.161
  node2: 192.168.88.162
  node3: 192.168.88.163

### 大数据服务启动与停止命令

以下是常用大数据组件的启动和停止命令，需根据实际部署路径调整。

#### Hadoop HDFS
- 启动 HDFS：`start-dfs.sh`
- 停止 HDFS：`stop-dfs.sh`

#### Yarn
- 启动 Yarn：`start-yarn.sh`
- 停止 Yarn：`stop-yarn.sh`
- **历史服务器**（需单独启动/停止）：
  - 启动：`mapred --daemon start historyserver`
  - 停止：`mapred --daemon stop historyserver`

#### Hive
- **Hive Metastore 服务**：
  ```bash
  cd /export/server/hive
  nohup bin/hive --service metastore >> logs/metastore.log 2>&1 &
### Spark
  ```bash
  cd /export/server/spark      sbin/start-all.sh   sbin/stop-all.sh
  sbin/start-thriftserver.sh --hiveconf hive.server2.thrift.port=10000 --hiveconf hive.server2.thrift.bind.host=node1 --master local[*]
  可在node2上执行sbin/start-master.sh,防止唯一master被杀死
