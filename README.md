# bigDataLearning
大数据学习之旅


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
