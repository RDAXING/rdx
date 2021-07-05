import java.util.Properties

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object SparkSqlReadHIve {
  {
    val dialect = new HiveSqlDialect
    JdbcDialects.registerDialect(dialect)
  }
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlReadHIve")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val url = "jdbc:hive2://cdh2.fahaicc.com:10000/tmpdb";
    val username = "hue";
    val password = "fahaihue!@#456";
    val driverName = "org.apache.hive.jdbc.HiveDriver";
    val properties = new Properties()
    properties.put("driver",driverName);
    properties.put("user",username);
    properties.put("password",password);
    val sourceRdd: DataFrame = session.read.jdbc(url, "test_demo", properties)
    sourceRdd.createOrReplaceTempView("hivetable");
//    sourceRdd.sqlContext.sql("select `test_jdcf.punishbasis` as punishbasis,count(1) as num from hivetable where punishbasis != null group by punishbasis").show(10);
//    sourceRdd.sqlContext.sql("select `test_jdcf.rowkey`,`test_jdcf.complainant`,`test_jdcf.sourceid`,`test_jdcf.complaintmatter`,`test_jdcf.punishbasis` as punishbasis,`test_jdcf.punishdaten`,`test_jdcf.authority`,`test_jdcf.bodyn` from hivetable where punishbasis is not null group by punishbasis").show(10);
//    sourceRdd.sqlContext.sql("select `test_jdcf.rowkey`,`test_jdcf.complainant` from hivetable").show();

    val frame: DataFrame = sourceRdd.sqlContext.sql(
      """
select `test_demo.source`,count(`test_demo.rowkey`) from hivetable group by `test_demo.source`
      """.stripMargin)
    frame.show()
  }
}

class HiveSqlDialect extends  JdbcDialect{
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

  override def quoteIdentifier(colName: String): String = colName
}