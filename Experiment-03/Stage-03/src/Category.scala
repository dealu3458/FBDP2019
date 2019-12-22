import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._


import java.util.Properties


object Category {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyJbdcDriverLogCount").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/dealu_si/Downloads/million_user_log.csv").map(_.split(","))
    val spark = SQLContext.getOrCreate(sc)
    val myschema = StructType(List(StructField("user_id", DataTypes.StringType), StructField("item_id", DataTypes.StringType),
      StructField("cat_id", DataTypes.StringType),StructField("merchant_id", DataTypes.StringType),StructField("brand_id", DataTypes.StringType),
      StructField("month", DataTypes.StringType),StructField("day", DataTypes.StringType),StructField("action", DataTypes.StringType),StructField("age_range", DataTypes.StringType),
      StructField("gender", DataTypes.StringType), StructField("province", DataTypes.StringType)))
    val rowRDD = lines.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)))

    val df = spark.createDataFrame(rowRDD,myschema)

    df.show()

    df.createOrReplaceTempView("tmp")
    val srch_prov = "四川"
    
    // 统计各省销售最好的产品类别前十（销售最多前10的产品类别）
    spark.sql("select * from (select cat_id, count(action) as tp from tmp where province=srch_prov and action='2' group by cat_id)t by count(action) desc limit 0,10").show()

    // 统计各省的双十一前十热门销售产品（购买最多前10的产品）
    spark.sql("select * from (select item_id, count(action) as tp from tmp where province=srch_prov and action='2' group by item_id)t by count(action) desc limit 0,10").show()

    // 查询双11那天浏览次数前十的品牌
    spark.sql("select * from (select brand_id, count(action) as tp from tmp action='0' group by brand_id)t order by count(action) desc limit 0,10").show()
  }
}
