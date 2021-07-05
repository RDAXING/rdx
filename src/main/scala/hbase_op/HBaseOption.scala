package hbase_op

import java.io.IOException
import java.sql.DriverManager
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.util.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
object HBaseOption {
  val connection: Connection = init()
//  private val connection: Connection = init() _
//  def main(args: Array[String]): Unit = {
//    val tableName:String = "SRV:KTGG"
//    val insetTable:String = "TEST:ARISKDEMO"
//    val rowkey:String = "00_AWLeFZViG1Ni0YFNKz4h,00_AWRpsbeklR_-GRxIxaYB,00_c-1xin0102minchu5269_t20190820,00_c02018441322minchu2996_t20180829,00_c02020lu1723minchu101_t20200214"
//
////    val table: Table = connection.getTable(TableName.valueOf(tableName))
////    val get: Get = new Get(Bytes.toBytes(rowkey))
////    get.setCacheBlocks(false)
////     = table.get(get)
////    val result: Result = getOneData(tableName,rowkey)
//    val list: List[String] = rowkey.split(",").toList
//    val results: Array[Result] = getDatas(tableName,list,"","")
////    val results: Array[Result] = getDatas(tableName,list,"STA","caseNoMD5")
//    for(result <- results){
//      //        (result,insetTable)
//      val res: String = insertDataToHbase(result,insetTable)
//      println(res)
////      val scanner: CellScanner = result.cellScanner()
////      while(scanner.advance()){
////        val cell: Cell = scanner.current()
//////        val cf: String = Bytes.toString(CellUtil.cloneFamily(cell))
//////        val family: String = Bytes.toString(CellUtil.cloneQualifier(cell))
//////        val value: String = Bytes.toString(CellUtil.cloneValue(cell))
//////        println(s"列族：${cf},列名：${family},值：${value}")
////        //写入数据到hbase中
////        val family: Array[Byte] = CellUtil.cloneFamily(cell)
////        val column: Array[Byte] = CellUtil.cloneQualifier(cell)
////        val value: Array[Byte] = CellUtil.cloneValue(cell)
//
////      }
//    }
//  }

  def main(args: Array[String]): Unit = {
    val tableName = "TEST:ARISKDEMO"
    getAllData(tableName)
  }
  /**
    * 初始化hbase Conn
    */
  def init():Connection={
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "cdh1.fahaicc.com,cdh2.fahaicc.com,cdh3.fahaicc.com")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, String.valueOf(1 << 30)) //1g
    ConnectionFactory.createConnection(conf)
  }

  def getOneData(tableName:String,rowkey :String) : Result ={
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowkey))
    get.setCacheBlocks(false)
    table.get(get)
  }


  /**
    * 批量查询
    * @param tablName 表名
    * @param rowkeys
    * @param family 列族
    * @param columns 列名 用‘，’进行分割
    */
  def getDatas(tablName:String,rowkeys:List[String],family:String ,columns:String):Array[Result]={
    val table: Table = connection.getTable(TableName.valueOf(tablName))
    val gets: ListBuffer[Get] = new ListBuffer[Get]
    for(rowkey <- rowkeys){
      val get = new Get(Bytes.toBytes(rowkey))
      if (!family.equals("") && !columns.equals("")) {
        val cols: Array[String] = columns.split(",")
        for (col <- cols) {
          get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col))
        }
      }
      get.setCacheBlocks(false)
      gets.+=(get)
    }
    val getsofJava: util.List[Get] = gets.asJava
    table.get(getsofJava)
  }

  /**
    * 写入数据到hbase中
    *
    */
  def insertDataToHbase(result:Result,tableName:String):String={
    val table: Table = init().getTable(TableName.valueOf(tableName))
    val rowkey: Array[Byte] = result.getRow
    val scanner: CellScanner = result.cellScanner()
    while(scanner.advance()){
      val put = new Put(rowkey)
      val cell: Cell = scanner.current()
      //        val cf: String = Bytes.toString(CellUtil.cloneFamily(cell))
      //        val family: String = Bytes.toString(CellUtil.cloneQualifier(cell))
      //        val value: String = Bytes.toString(CellUtil.cloneValue(cell))
      //        println(s"列族：${cf},列名：${family},值：${value}")
      //写入数据到hbase中
      val family: Array[Byte] = CellUtil.cloneFamily(cell)
      val column: Array[Byte] = CellUtil.cloneQualifier(cell)
      val value: Array[Byte] = CellUtil.cloneValue(cell)
      put.addColumn(family,column,value)
      table.put(put)
    }
    "ok"
  }


  //只传表名得到全表数据  : ListBuffer[String]
  def getAllData(tableName: String) = {
    var table: Table = null
    val list=new ListBuffer[String]
    try {
      table = connection.getTable(TableName.valueOf(tableName))
      val results: ResultScanner = table.getScanner(new Scan)
      import scala.collection.JavaConversions._
      for (result <- results) {
        for (cell <- result.rawCells) {
          val row: String = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
          val family: String = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
          val colName: String = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          val context: String = "rowkey:" + row +","+ "列族:" + family +","+ "列:" + colName +","+ "值:" + value
//          list+=context
          println(context)
        }
      }
//      results.close()
    } catch {
      case e: IOException =>e.printStackTrace()
    }
//    list
  }
}

import scala.util.control.Breaks._

object SourceData{
  def main(args: Array[String]): Unit = {
    //获取mysql中的数据
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SourceData")
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val sql:String = "select * from datataskinfor_bak where dataTaskId >= ? and dataTaskId <= ?"
    val resultRdd: JdbcRDD[(Int, String)] = new JdbcRDD(sc, () => {
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/testdata?useUnicode=true&characterEncoding=UTF8&useSSL=false&serverTimezone=UTC", "root", "")
    }, sql, 370, 390, 2, result => {
      val dataTaskId: Int = result.getInt("dataTaskId")
      val dataTaskName: String = result.getString("dataTaskName")
      (dataTaskId, dataTaskName)
    })
    println(resultRdd.count())
    resultRdd.foreach(f =>{
      println(f)
    })

  }
}