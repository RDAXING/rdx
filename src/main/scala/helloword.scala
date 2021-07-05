import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object helloword {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setAppName("SparkESOps").setMaster("local")
    //在spark中自动创建es中的索引
    conf.set("cluster.name", "Apollo-CBD")
    conf.set("es.read.metadata", "true")
    //        conf.set("es.index.auto.create","true");
    conf.set("es.nodes", "121.52.212.147")
    conf.set("es.port", "9200")
    conf.set("es.nodes.wan.only", "true")
    val sc = new SparkContext(conf)
    val unit = EsSpark.esJsonRDD(sc,"company_qcc")


  }
}

object demo111{
  def main(args: Array[String]): Unit = {
    for(i <- 1 to 10){
      print(i + "hahah + \n")
    }
  print("*******************************")
    for(i <- 1 until 10){
      println(i + "h")
    }

    val lis = List("a",10,"lalal","spark");
    for(i <- lis){
      println(i)
    }

    for(i<- 1 to 20 if i%2 == 0){
      println("守卫："+i)
    }


    for(i <- 1 to 20 ; j=4-i){
      println("j="+j)
    }

    //嵌套循环
    for(i <- 1 to 10;j<- 1 to 4){
      println("嵌套循环:"+i + ":" +j)
    }

    //循环并返回值
    val ints = for(i <- 1 to 20) yield i
    println("循环并返回值:"+ints)
    val res = for(i <- 1 to 20 ) yield {
      if (i % 2 == 0) {
        i
      } else {
        "不是偶数"
      }
    }

    println(res)
  for( i<- Range(1,10,2)){
    println("步长："+i)
  }
  }

}

//object  demo2{
//  def main(args: Array[String]): Unit = {
//    //scala实现break方法
//    breakable{
//      var i = 0
//      while(i<10){
//        if(i==8){
//          break()
//        }
//        println(i)
//        i+=1
//      }
//    }
//
//    //scala实现continue
//    for(i <- 1 to 20){
//      breakable{
//        if(i %2 == 1){
//          break()
//        }else{
//          println("continue:"+i)
//        }
//      }
//    }
//  }
//}