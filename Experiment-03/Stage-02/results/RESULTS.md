# 阶段2结果

##  查询双11那天有多少人购买了商品
将精简数据集导入Hive后（导入为 TABLE ex_demo)，运行：
> SELECT DISTINCT user_id </br> FROM ex_demo </br> WHERE action="2";

结果为
> 37,202人在双11购买了商品

## 查询双11那天男女买家购买商品的比例
将精简数据集导入Hive后（导入为 TABLE ex_demo)，运行：
> SELECT DISTINCT user_id </br> FROM ex_demo </br> WHERE gender=<#gender>;

* gender="0" 得到女性用户总数：34,619
* gender="1" 得到男性用户总数：34,556

运行:
> SELECT DISTINCT user_id </br> FROM ex_demo </br> WHERE gender=<#gender> AND action="2";

* gender="0" 得到女性购买者总数：22,477，占女性用户总数64.93%
* gender="1" 得到男性购买者总数：22,413，占男性用户总数64.86%

> 不清楚为什么男女性购买者总数加和大于总购买人数，由于Hive代码无误，只能认为是原数据集存在某一user_id存在gender不同的情况

## 查询双11那天浏览次数前十的品牌
将精简数据集导入Hive后（导入为 TABLE ex_demo)，运行：
> CREATE TABLE tmp(brand_id String, viewing int); <br> // 建立临时表tmp存储品牌ID及其浏览量数据

> DESCRIBE tmp; </br> // 查看数据格式是否正确

> INSERT INTO tmp </br> SELECT brand_id, COUNT(action) </br> FROM ex_demo </br> WHERE action="0" </br> GROUP BY brand_id;
</br> // 将品牌ID和浏览数从 ex_demo 插入 tmp

> SELECT * </br> FROM tmp </br> ORDER BY viewing </br>  DESC </br> LIMIT 0, 10;
</br> // 查询tmp中按浏览量排序最靠前的10条数据

结果为：
> 1360	49151 </br> 3738	10130 </br> 82	9719 </br> 1446	9426 </br> 6215	8568 </br> 1214	8470 </br> 5376	8282 </br> 2276	7990 </br> 1662	7808 </br> 8235	7661
