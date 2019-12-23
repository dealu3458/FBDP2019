# 阶段3：基于Spark完成对精简数据集产品、类别、品牌分析

## src
使用IntelliJ IDEA并基于Spark操纵DataFrame编程

## screenshots
IntelliJ IDEA代码界面、Spark-shell Scala输出截图

## results
随机选取五个省份（上海、台湾、四川、甘肃、重庆）运行本程序后得到的结果
* <province-cat> 为该省份Top 10热门销售产品 </br> 
* <province-item> 为该省份Top 10热门产品 </br> 
* <Brand> 为浏览次数前十的品牌

经过比较，基于Spark和MapReduce得到的该省份Top 10热门产品相同，基于Spark和Hive得到的浏览次数前十品牌也相同
