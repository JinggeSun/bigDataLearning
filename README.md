# bigDataLearning
大数据学习之旅


# 大数据安装
## Linux服务器
准备2个Linux服务器，我采用的是2台虚拟机，操作系统centos
1. 创建2个centos，分别作为master，slave
### 网络主机名
##### 设置静态IP地址
1. ip addr 查看网卡和ip地址
2. vi /etc/sysconfig/network-scripts/ifcfg-eth0(自己的网卡)
3. BOOTPROTO="dhcp"  =》static
4. 失败。设置静态ip地址后，无法ping通外网和其他主机
##### 关闭防火墙
1. firewall-cmd --state  
2. systemctl stop firewalld.service  
3. systemctl disable firewalld.service
##### 设置主机名
1. /etc/sysconfig/network
2. HOSTNAME=master
3. /etc/hosts
4. ip地址 master
##### ssh
1. /etc/ssh/sshd_config
2. PublicAuthentication yes 去掉注视
3. ssh-keygen -t rsa
4. cat ~/.ssh/id_rsa.pub>> ~/.ssh/authorized_keys  将master机器公钥写入authorized_keys
5. ssh root@slave cat ~/.ssh/id_rsa.pub>> ~/.ssh/authorized_keys 登陆slave，将slave的公钥写入master中
6. ssh root@master cat ~/.ssh/authorized_keys>> ~/.ssh/authorized_keys 登陆master，将master的公钥写入slave中
7. ssh master/slave 测试
##### jdk8
1. 下载jdk8
2. 解压 /usr/lib
3. /etc/profile 配置
3. source
4. java -version
```
export JAVA_HOME=/usr/java/jdk1.8.0_144
export CLASSPATH=.:${JAVA_HOME}/jre/lib/rt.jar:${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar
export PATH=$PATH:${JAVA_HOME}/bin
```
##### hadoop包
1. 参考官网集群安装
2. 下载版本：3.2.1
3. /opt/ 目录下
4. master:namenode,slave:datanode
5. /etc/hadoop/hadoop-env.sh
6. /etc/hadoop/yarn-env.shexport JAVA_HOME=/usr/java/jdk1.8.0_144
7. 第5条和第六条配置 export JAVA_HOME=/usr/java/jdk1.8.0_144
8. /etc/hadoop/core-site.xml
9. /etc/hadoop/hdfs-site.xml
10. /etc/hadoop/mapred-site.xml
11. /etc/hadoop/yarn-site.xml
12. /etc/hadoop/slavesv
```
将start-dfs.sh，stop-dfs.sh两个文件顶部添加以下参数
              HDFS_DATANODE_USER=root
              HADOOP_SECURE_DN_USER=hdfs
              HDFS_NAMENODE_USER=root
              HDFS_SECONDARYNAMENODE_USER=root
start-yarn.sh，stop-yarn.sh顶部也需添加以下
            YARN_RESOURCEMANAGER_USER=root
            HADOOP_SECURE_DN_USER=yarn
            YARN_NODEMANAGER_USER=root
```
###### hadoop
1. 配置环境变量
2. http://master:8088/
3. http://master:9870
##### hadoop2与hadoop3不同
1. 50070 端口改为 9870
2. 集群不在slaves要在works配置
##### 开始关闭
1. start.all.sh
2. stop.all.sh
3. master/slave jps

## Hive
hive-3.1.2

Hive是基于Hadoop的一个数据仓库工具，可以将结构化的数据文件映射为一张数据库表。Hive 定义了简单的类 SQL 查询语言，称为 HQL，它允许熟悉 SQL 的用户查询数据。

mysql存储元数据，数据仓库的数据存放在hdfs中

Hive的本质是将 SQL 语句转换为 MapReduce 任务运行
1. 内嵌安装
2. 本地安装
3. 远程安装
### mysql
linux安装mysql
1. wget -i -c http://dev.mysql.com/get/mysql57-community-release-el7-10.noarch.rpm
2. yum -y install mysql57-community-release-el7-10.noarch.rpm
3. yum -y install mysql-community-server
1. systemctl start mysqld

##### 安装
1. 解压 /opt
2. 配置环境变量
```
export HIVE_HOME={{pwd}}
export PATH=$HIVE_HOME/bin:$PATH

```
3. 将mysqll jar上传到 hive文件夹$HIVE_HOME/lib
4. 配置文件 mysql4大项
5. schematool -dbType mysql -initSchema 初始化元数据
6. hive运行

### Hive 数据类型
1. 原始数据类型
2. 整型，布尔型，字符串，浮点型
3. 复杂数据类型
4. maps，arrays，structs
```
CREATE TABLE complex(

         col1 ARRAY< INT>,

         col2 MAP< STRING,INT>,

         col3 STRUCT< a:STRING,b:INT,c:DOUBLE>

)
```
5. TEXTFILE 文本，默认值
6. SEQUENCEFILE 二进制序列文件
7. RCFILE 列式存储格式文件 Hive0.6以后开始支持
8. ORC 列式存储格式文件，比RCFILE有更高的压缩比和读写效率，Hive0.11以后开始支持
9. PARQUET 列出存储格式文件，Hive0.13以后开始支持
### Hive
1. show databases;
2. create database 数据库名 comment 'database for test';
3. describe database 数据库名;
4. use 数据库名;
5. drop database 数据库名;
6. create table(id INT comment desc)
7. DESC 表名;
8. DROP TABLE 表名;
9. show create table tablename;
10. drop掉内部表，那么这个表的元数据和数据会被一起删掉。建表时建表语句中没有加 [EXTERNAL]的表称为内部表或者管理表
11. 删除表时并不会删掉数据本身 外部表




## 软件安装
### MongoDB
MongoDB 是一个基于分布式文件存储的数据库。

使用docker安装MongoDB
1. docker pull mongo
2. docker run -p 27017:27017 -v $PWD/db:/data/db -d mongo:latest
###### 客户端
建议使用客户端，可以熟悉一下MongoDB
1. docker run -it mongo:3.2 mongo --host 172.17.0.1
###### web可视化
1. git clone https://github.com/mrvautin/adminMongo    
2. npm install
3. npm start

### ES
ES 安装参考官网

### spark
spark 安装参考官网


## 电影推荐
这个是从b站上搜索的大数据项目视频，跟着视频一起学习。比较蛋疼的是只有视频，没有课件。代码在github上搜索的，竟然有源代码，意外。
