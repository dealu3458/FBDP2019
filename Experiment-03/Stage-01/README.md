# 阶段1：基于MapReduce完成对精简数据集产品热度、销售情况分析

## src

### fileReader.java
使用fileReader.class读入输入的数据集，数据集的每一条log为一个fileReader类。

### Sales.java
能统计某一省份的双十一热门销售产品，并按数据集中item_id出现顺序输出。打包后，运行：
> hadoop jar Sales.jar #file input path #file output path #province

注意：province需要以汉字形式输入

### Hot.java
能统计某一省份的双十一热门产品（即浏览点击、关注、加入购物车、购买总数），并按数据集中item_id出现顺序输出。打包后，运行：
> hadoop jar Hot.jar #file input path #file output path #province

注意：province需要以汉字形式输入。得到各产品在各省份的销售/热度情况后，需导入Excel/Matlab/Hive/MySQL中进行排序分析。
> 考虑到数据量不到20000行，使用MapReduce反而会降低效率

## screenshots
IntelliJ IDEA代码界面、运行和从HDFS拷贝回本地的中间结果截图

## results
随机选取十个省份（安徽、澳门、重庆、福建等）运行本程序后得到的结果。
> <province.png> 为该省份Top 10热门销售产品
> <province-hot.png> 为该省份Top 10热门产品
