import java.io.File

import com.google.gson.annotations.Until

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.{BufferedSource, Source}
import scala.util.Random

object ConnFactory{
  private val name : String = "cpws"
  private val lines: List[String] = Source.fromFile("C:\\Users\\admin\\Desktop\\sparktest\\a.txt").getLines().toList

  private val list1: List[Array[String]] = lines.map(_.split(" "))
  private val list2: List[(Char, Char)] = lines.map(f =>(f(0),f(1)))

  def main(args: Array[String]): Unit = {
    //定义数组
    val arr = new Array[Int](3)
    arr(0)=10
    arr(1)=9
    arr(2) = 6
    for(i<-arr){
      print(i)
    }


    val lines = "a,d,c,f,r,h".split(",")
    for(i<- lines){
      println(i)
    }

    for(i<- 0 until lines.length){
      println(s"当前数：${lines(i)}")
    }

    val ints = Array(1,2,3,4,5,6)
    for(i <- ints){
      println(i)
    }


    //可变数组的定义
    val arrs = new ArrayBuffer[String]()
    //追加单个元素
    arrs.append("hello")
    arrs.+=("spark")
    arrs.+=:("hive")

    //追加数组
    val a1 = Array("java","scala")
    arrs.++=(a1)
    for(i<-arrs){
      println("可变数组:"+i)
    }
    val arr01 = ArrayBuffer[Any](3, 2, 5)
    val array = arr01.toArray
    println(array)
    val buffer = array.toBuffer
    println(buffer)

    //元组
    val tumple = (1,2,3,4,"scala",Array(1,2,3,4,5),1.2)
    println(s"${tumple._1},${tumple._2},${tumple._3},${tumple._4},${tumple._5},${tumple._6},")
    for(i <- tumple.productIterator){
      println("元组："+i)
    }

    //list
    println("LIst_________________________________")
    val list = List(1,2,3,"helol")//定义一个定长的List
    val list2 = Nil;//定义一个空的List集合
    val list3 = list.::("hava")
    val seq = "hahha".+:(list)
    println("seq:"+ seq)
    val list4 = list.:+("hive")
    println("list4:" + list4)
    val strings = list2.::("spark")

    val list7 = 4::5::6::list4
    val list8 = 4::5::6::list4:::list3
    println("list7:"+list7)
    println("list8:"+list8)
    println(list3.size)
    println(strings.size)

    //可变的List
    val li = ListBuffer(1,2,3,4)//定义一个含有初始值的可变List
    li.append(5,7,8)
    println(li)

    val li2 = new ListBuffer[String]//定义一个空的可变List
    li2.append("java")
    li2.+=("spark")

    println(li2)
    li2.remove(1)
    println(li2)


  }

}

class PP11{

}

object Queued{
  def main(args: Array[String]): Unit = {
    val queue = new mutable.Queue[Int]()//定义一个空的队列
    queue+=5//往队列中追加单个元素
    queue++=List(1,2,3,4)//往队列中添加集合
    println(queue.size)
    for(i<-queue){
      println(i)
    }
    for(i<- 0 until queue.size){
      val i1 = queue.dequeue()//从队列中进行取出元素
      println(s"取出第${i+1}个元素：${i1}")
    }

    println(queue.size)//当前队列的元素为0

    queue.enqueue(1,2,3,4)//往队列中添加元素
    println(queue.size)
println("(((((((((((((((((以下的操作对队列没有影响(((((((((((((((((((")
    println(queue.head)//1
    println(queue.last)//4
    //取出队尾的数据 ,即：返回除了第一个以外剩余的元素，可以级联使用
    println(queue.tail)//Queue(2, 3, 4)
    println(queue.tail.tail)//Queue(3, 4)
    println(queue.tail.tail.tail)//Queue(4)

    println(queue.size)//4
  }
}


object MapD{
  def main(args: Array[String]): Unit = {
    val map = Map("one"->1,"two"->2,"list" ->List(11,12,34))//默认map是不可变的
    println(map)
   val unit = map("one")//获取值
    val value = map.get("list").get//获取值的方式二
    println(value)

    //遍历方式1
    map.foreach(f =>{
      println("key:"+f._1 + ",value:" +f._2)
    })

    //遍历方式2
    for((k,v) <- map){
      println("key:"+k + ",value:" +v)
    }

    //遍历方式3
    for(i<- map.keys){//也可以直接获取values的值：map.values
      println(s"key:${i},value:${map.get(i).get}")
    }

    val it = map.keySet.iterator
    while(it.hasNext){
      val key = it.next()
      val value1 = map.get(key).get
      println(s"key:${key},value:${value1}")
    }
    if(map.contains("hello")){

      val unit1 = map("hello")
      println(unit1)
    }else{
      println("hhh")
    }
  }

}

object  MapD1{
  def main(args: Array[String]): Unit = {
    //定义可变的map
    val map = scala.collection.mutable.Map[String,String]()
    //往map中添加数据  当增加一个 key-value ,如果 key 存在就是更新，如果不存在，这是添加
    map+=("one"->"java")
    map+=("two"->"hive")
    map+=("three"->"spark")
    map+=("four"->"hbase")

    println(map)
    //获取指定的值
    val v1 = map("one")
    println(v1)

    //删除map中的数据 删除一个或者多个
    map-="one"
    map-=("two","three")
    println(map)
  }

}


object SetD{
  def main(args: Array[String]): Unit = {
    //定义不可遍历的集合Set
    val set = Set(1,2,3,3,"java")
    for(i<- set){
      println(i)
    }

    //定义一个可变的集合Set
    val set1 = mutable.Set[String]()
    set1+="halle"//添加元素
    set1+="java"//添加元素
    set1+="spark"//添加元素
    set1+="hive"//添加元素
    set1+="scala"//添加元素
    println(set1.size)

    //删除元素
    set1-="halle"
    println(set1)//Set(hive, java, spark, scala)
    val list = Array(1,2,34).toList //数组转换为list
  }

}
object  ALLD{
  def main(args: Array[String]): Unit = {
    val ints = List(1,2,3,4)
    val ints1 = ints.map(_*2)
    val ints2 = ints.map(f=>f<<1)
    println(ints1)
    println(ints2)
  }
}

object DDM{
  def main(args: Array[String]): Unit = {
    //获取文件
    val list = Source.fromFile("C:\\Users\\admin\\Desktop\\sparktest\\a.txt").getLines().toList
    println(list.size)
    val res = list.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.size)
    println(res)


    val num = List(1,2,3,4,5,6)
    val sum = (a:Int,b:Int) => a+b
    val resq = num.reduceLeft(sum)
    println(resq)

    val su = num.reduce(sum)
    println(s"reduce:${su}")
    val rr = num.reduceRight(sum)
    println(s"reightR:${rr}")

    val fold1 = num.fold(2)(sum)
    val fr = num.foldRight(2)(sum)
    val ff = num.foldLeft(2)(sum)
    println(s"f1:${fold1},fr:${fr},ff:${ff}")//f1:23,fr:23,ff:23
  }
}

object StreamD{
  def main(args: Array[String]): Unit = {
    def numsForm(n: BigInt) : Stream[BigInt] = n #:: numsForm(n + 1)
    val stream1 = numsForm(1)
    println(stream1) //
    //取出第一个元素println("head=" + stream1.head) //
    println(stream1.head)
    println(stream1.tail)
  }
}


object parD{
  def main(args: Array[String]): Unit = {
    val distinct1 = (0 to 100).map{case _ => Thread.currentThread().getName}.distinct
    println(distinct1)


    val result2 = (0 to 100).par.map{case _ => Thread.currentThread.getName}.distinct
    println(result2)

  }
}

object  MatchD{
  def main(args: Array[String]): Unit = {
    /*模式匹配*/
    val list = List("A","B","C","D")
    val value = list(Random.nextInt(list.size))
    println(value)
    value match {
      case "A" => println("a")
      case "B" => println("result B")
      case "C" => println("C")
      case "D" => println("D")
      case _ =>println("other result")
    }

    val list11 = List(1, 2.0, "3", Array[Int](5,6))
    val data = list11(Random.nextInt(list.size))
    //val data = list(3)
    println(data)
    var res:Int = -1
    // 通过match 匹配类型
    data match {
      case x:Int => res = x
      case x:Double => res = x.toInt
      case x:String => res = x.toInt
      // 匹配数组
      case x:Array[Int] => res = x.sum
    }

    println(s"res:${res}")

    val arr = Array(1, 2, 4)
    arr match {
      case Array(1, x, y) => println(s"x:$x,y:$y")
      case Array(_, x, y, d) => println(s"x:$x,y:$y,d:$d")
      case _ => println("other")
    }


  }
}

object MD{
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5)
    val ints = list.map((x:Int) => x*2)

    println(ints)
    val ints1 = list.map({case x:Int => x*2})
    println(ints1)

    val list1 = List(1,2,3,"aa",5,6)
//    val ints2 = list1.map({case x:Int => x*2})
//    println(ints2)
    val ints3 = list1.collect({case x:Int => x*2})
    println(ints3)


  }
}


object CollectH{
  def main(args: Array[String]): Unit = {
    //集合的高级操作
    //(1)sorted sortBy
    val ints = List(1,3,67,2,4,65,34,23,52)
    val sorted: List[Int] = ints.sorted
    println(sorted)//List(1, 2, 3, 4, 23, 34, 52, 65, 67)

    val tuples = List((1,"java"),(2,"scala"),(4,"spark"),(3,"hive"))
    val tuples1: List[(Int, String)] = tuples.sortBy(_._1)
    println(tuples1)//List((1,java), (2,scala), (3,hive), (4,spark))


    //(flatten 不支持元组)
    val intses: List[List[Int]] = List(List(1,2,3,4),List(5,6,7,8))
    val flatten: List[Int] = intses.flatten
    println(flatten)//List(1, 2, 3, 4, 5, 6, 7, 8)

    //grouped groupBY
    val list = List("a",1,"b",2,"c","3")
    val lists: Iterator[List[Any]] = list.grouped(2)
    print("group1:"+lists)
    print("group2:"+lists.toBuffer)
    print("group3:"+lists)

    //
    val map = Map("b" -> List(1,2,3,4), "a" -> List(4,5,6,7))
    val stringToInt: Map[String, Int] = map.mapValues(_.sum)
    println(stringToInt)

    val intses1 = List(List(1,2), List(1,2,3),List(1,2,3,4), List(1))
    val i: Int = intses1.aggregate(0)(_+_.sum,_+_)
    println(i)

    //wordCount统计
    val word = List("a b c d","a d e s","a b d e")
    val result: Map[String, Int] = word.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.map(_._2).sum)
    println(s"result:${result}")//result:Map(e -> 2, s -> 1, a -> 3, b -> 2, c -> 1, d -> 3)
  }
}




