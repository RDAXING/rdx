import java.sql.Driver
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf.LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE
import scala.collection.mutable.ListBuffer

object RDDToDF {
  case class Person(id:Int ,name:String,age:Int)

  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddtodf")
//    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val session: SparkSession = SparkSession.builder().master("local[*]").appName("rddtodf").getOrCreate()
    import session.implicits._
    val path = "C:\\Users\\admin\\Desktop\\sparktest\\sparksql\\demo\\person.txt"
    dynamicChange(session,path)

  }

  /**
    * 动态转换
    * @param session
    * @param path
    */
  def dynamicChange(session:SparkSession,path:String):Unit = {
    //获取文件中的数据
    val dataRdd: RDD[String] = session.sparkContext.textFile(path)
    import session.implicits._
    //结构字段
    val schemaString = "id,name,age"
    //生成结构属性
    val fields: Array[StructField] = schemaString.split(",").map(filedName => {
      StructField(filedName, StringType, nullable = true)
    })
    //生成结构类型
    val structType: StructType = StructType(fields)
    //将对dataRdd中的数据进行转化为Row类型
    val rowRdd: RDD[Row] = dataRdd.map(_.split(",")).map(field =>Row(field(0),field(1),field(2)))
    //rdd转化为df
    val frame: DataFrame = session.createDataFrame(rowRdd,structType)
    frame.printSchema()

    frame.createOrReplaceTempView("person")
    frame.sqlContext.sql(
      """
        |select * from person
      """.stripMargin).show()
  }
}

object RddDf{
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("RDDTODFS").getOrCreate()
//    import session.implicits._
//    session.sparkContext
    val path = "C:\\Users\\admin\\Desktop\\sparktest\\sparksql\\demo\\person.txt"
    val dataRdd: RDD[String] = session.sparkContext.textFile(path)
    val schemaString = "id,name,age"
    val fields: Array[StructField] = schemaString.split(",").map(f => StructField(f,StringType,nullable = true))
    val structType: StructType = StructType(fields)
    val rowRdd: RDD[Row] = dataRdd.map(_.split(",")).map(f =>Row(f(0),f(1),f(2)))
    val df: DataFrame = session.createDataFrame(rowRdd,structType)
    df.printSchema()
    df.createOrReplaceTempView("person")
    df.sqlContext.sql(
      """
        |select name from person
      """.stripMargin).show()

    val rdd: RDD[Row] = df.rdd
    rdd.foreach(f =>{
      println(f.getAs[String](1))
    })
  }
}

object DftoRdd{
  def main(args: Array[String]): Unit = {
    val path = "C:\\Users\\admin\\Desktop\\sparktest\\sparksql\\demo\\person.txt"
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val rdd: RDD[String] = sc.textFile(path)
    val schemaString = "id,name,age"
    val fieldsStruct: Array[StructField] = schemaString.split(",").map(fields =>StructField(fields,StringType,nullable = true))
    val structType: StructType = StructType(fieldsStruct)
    val rowRdd: RDD[Row] = rdd.map(_.split(",")).map(f=>Row(f(0),f(1),f(2)))
    val session = new SQLContext(sc)
    val df: DataFrame = session.createDataFrame(rowRdd,structType)
    df.printSchema()
    /*
    * root
 |-- id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)*/
    df.show()
    /*
    +---+--------+----+
| id|    name| age|
+---+--------+----+
|  1|zhangsan|20  |
|  2|    lisi|21  |
|  3|  wanger|19  |
|  4| fangliu|  18|
+---+--------+----+
    * */
  import session.implicits._

  }

}

object DS{
  {println("heool")}
  def main(args: Array[String]): Unit = {
    //sparksql读取mysql中的数据
    val session: SparkSession = SparkSession.builder().master("local[*]").appName("ds").getOrCreate()
    import session.implicits._
    val dataF: DataFrame = session.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true")
      .option("dbtable", "datataskinfor_bak")
      .option("user", "root")
      .option("password", "").load()
    dataF.printSchema()
    val schema: StructType = dataF.schema
    println(schema)
    dataF.show(30)

    val personDS: Dataset[Person] = dataF.map {
      case row: Row => Person(row.getAs[Int]("dataTaskId"), row.getAs[String]("dataTaskName"))
      case _ => Person(0, "None")
    }
    personDS.foreach(f =>{
      println(f)
    })


    val resultRdd: Dataset[Person] = dataF.mapPartitions(func => {
      val persons: ListBuffer[Person] = ListBuffer[Person]()
      while (func.hasNext) {
        val row: Row = func.next()
        persons.+=(Person(row.getAs[Int]("dataTaskId"), row.getAs[String]("dataTaskName")))
      }
      persons.iterator
    })
    resultRdd.foreachPartition(func =>{
      while(func.hasNext){
        val person: Person = func.next()
        println(person)
      }
    })

    dataF.select("dataTaskId").filter("dataTaskId>400").show()
    val frame: DataFrame = dataF.groupBy("dataTaskName").count()
    val finRdd: Dataset[String] = MergeField(frame)
    finRdd.show()
  }

  case class Person(id:Int,name:String)

  def MergeField(frame: DataFrame):Dataset[String]={
    implicit val encoder=org.apache.spark.sql.Encoders.STRING//添加字符串类型编码器
    val resultRdd: Dataset[String] = frame.mapPartitions(func => {
      val rows: ListBuffer[String] = ListBuffer[String]()
      while (func.hasNext) {
        val row: Row = func.next()
        val str: String = row.toSeq.foldLeft("")(_ + "," + _).substring(1)
        rows += str
      }
      rows.iterator
    })
    resultRdd
  }


}

object Ssql{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sql").getOrCreate()
    import spark.implicits._
    val dataf: DataFrame = spark.createDataFrame(List(
      ("mery", "f", "23"),
      ("tony", "f", "22"),
      ("tom", "m", "21"),
      ("tom", "", "21"),
      ("jary", "m", "29")
    )).toDF("name", "sex", "age")
//    dataf.show()
//    dataf.filter("sex != ''").show()
//    dataf.select("name","sex").show()
//    dataf.select("name").filter("age>=27").show()
//    dataf.groupBy("sex").agg(Map("age" ->"max","name" ->"count")).show()

    dataf.createGlobalTempView("person")
    spark.sql(
      """
        |select * from global_temp.person
      """.stripMargin).show()

    spark.sql(
      """
        |select name,sex from global_temp.person where sex != ''
      """.stripMargin).show()

    spark.sql(
      """
        |select sex ,max(age),count(name) from global_temp.person group by sex
      """.stripMargin).show()
  }
}

object ReadMysql2{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("ReadMysql2").getOrCreate()
    //	        map.put("driver","com.mysql.jdbc.Driver");
    //	        map.put("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
    //	        map.put("user","root");
    //	        map.put("password","");
    //	        map.put("dbtable","datataskinfor_bak");
    val option = Map(
      "driver" -> "com.mysql.jdbc.Driver",
      "url" -> "jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true",
      "user" -> "root",
      "password" -> "",
      "dbtable" -> "datataskinfor_bak"
    )
    import spark.implicits._
    val mysqlDataFrame: DataFrame = spark.read.format("jdbc").options(option).load()
    mysqlDataFrame.show()


  }
}

object readFile{
  def main(args: Array[String]): Unit = {
    val path = "C:\\Users\\admin\\Desktop\\sparktest\\sparksql\\demo\\person.txt"
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("ReadFile").getOrCreate()
    import spark.implicits._
    val sourRdd: RDD[String] = spark.sparkContext.textFile(path,6)
    print(sourRdd.getNumPartitions)
    val tupleRdd: RDD[Row] = sourRdd.mapPartitions(f => {
      val tuples: ListBuffer[Row] = ListBuffer()
      while (f.hasNext) {
        val str: String = f.next()
        val datas: Array[String] = str.split(",")
        val tuple: (String, String, String) = (datas(0), datas(1), datas(2))
        tuples.append(Row(tuple._1,tuple._2,tuple._3))
      }
      tuples.iterator
    })

    val schemaStr = "id,name,age"
    val fields: Array[StructField] = schemaStr.split(",").map(f => StructField(f,StringType,nullable = true))
    val structType: StructType = StructType(fields)

    val frame: DataFrame = spark.createDataFrame(tupleRdd,structType)
    frame.printSchema()
    frame.show()

    val writeJdbc = new Properties();
    writeJdbc.setProperty("driver","com.mysql.jdbc.Driver");
    writeJdbc.setProperty("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
    writeJdbc.setProperty("user","root");
    writeJdbc.setProperty("password","");
    writeJdbc.setProperty("dbtable","t_user");
    frame.write.mode(SaveMode.Append).jdbc(writeJdbc.getProperty("url"),"t_user",writeJdbc)
    println("ok")

  }
}