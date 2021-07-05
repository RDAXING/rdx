package mysqlutil

import scala.io.Source
class ImplicitD2(path :String){
  //读取文件
  def readFile():String ={
    println("调用的方法")
    Source.fromFile(path).mkString
  }
}


object ImplicitD2 {
  def main(args: Array[String]): Unit = {
    val path = "C:\\Users\\admin\\Desktop\\sparktest\\a.txt"
    //引入隐shi函数
    import mysqlutil.MyPredef.implicitD2ReadFile
    println(path.readFile())
    /*结果：
      * 隐藏函数
      调用的方法
      java spark scala
      hive hbase elasticsearch java spark scala
      hive hbase elasticsearch java spark scala
      hive hbase elasticsearch java java spark scala
      hive hbase elasticsearch java spark scala
      hive hbase elasticsearch java spark scala
      hive hbase elasticsearch hbase kafka*/


  }
}

class HNS(var name:String,var age:Int){

  override def toString: String = s"name:${name},age:${age}"
}


object HNS{

  def chooseCom(a:HNS,b:HNS):HNS={
    import mysqlutil.MyPredef.STUCOM
    val or: Ordering[HNS] = Ordering[HNS]
    if(or.gt(a,b)) a else b
  }

  def main(args: Array[String]): Unit = {
    val zhang = new HNS("zhang",23)
    val wang = new HNS("wang",32)
    val ren = new HNS("ren",11)
    val li = new HNS("li",45)
    val hnses = List(zhang,wang,ren,li)
    var hhs:HNS = zhang
    for(hns <- hnses){
      hhs = chooseCom(hhs,hns)
    }
    println(hhs)

  }
}

/**
  * 隐式参数和隐式值
在调用含有隐式参数的函数时，编译器会自动寻找合适的隐式值当做隐式参数，而只有用implict标记过的值、对象、函数才能被找到。
def add(x:Int)(implicit y:Int) = x + y
  */
object dem1{
  //定义一个隐式函数，将int转换为string
  implicit val intToString = (a:Int) => a.toString
  def main(args: Array[String]): Unit = {
    //隐式函数：将int转换为String
    val value = 1234
    println(value.length)

    //调用隐式函数最为方法的参数
    val str: String = say(value)
    println(str)

    implicit  val v = 100
    val i: Int = add(6)
    println(i)//106

  }

  //定义一个隐式参数，求相加
  def add(a:Int)(implicit b:Int):Int={
    a+b
  }

  //将隐式函数作为隐式参数
  def say(implicit  value :String ):String ={
    s"hell0 ${value}"
  }
}

object impobj{
  import  mysqlutil.MyPredef.ImplicitObj
  // 定义一个隐式参数是隐式对象
  def say(str:String)(implicit o:ImplicitObj.type ) = o.sayH(str)
  def main(args: Array[String]): Unit = {
    //直接调用隐式对象进行结果的输出
    val obj: MyPredef.ImplicitObj.type = implicitly[MyPredef.ImplicitObj.type ]
    val str: String = obj.sayH("spar")
    println(str)//hah:spar
    //当调用的时候，需要引入隐式对象，然后再作为隐式参数，不需要再添加 调用参数
    val str1: String = say("hive")
    println(str1)//hah:hive
  }
}

object IMP{
  //定义一个隐式方法
  implicit def intStr(n:Int) : String = n.toString()

  //定义一个隐式参数
  def add(a:Int)(implicit b:Int):Int = a+b
  def main(args: Array[String]): Unit = {
    val i: Int = 1234556
    println(i.length)
    //隐式参数的使用
    implicit val   in:Int = 209
    val i1: Int = add(4)
    println(i1)
    //
  }
}


object IMP2{
  def main(args: Array[String]): Unit = {
    val path : String = "C:\\Users\\admin\\Desktop\\sparktest\\a.txt"
    import mysqlutil.MyPredef.rdFile
    println(path.readFile())
  }
}

class RD(path:String){
  def readFile():String = {

    Source.fromFile(path).mkString
  }
}


class Person(var name:String,var age:Int){
  override def toString: String = s"name:${name},age:${age}"
}

object Person{
  def comparTwoP(x:Person,y:Person):Person = {
    import mysqlutil.MyPredef.PersonCom
    val p: Ordering[Person] = Ordering[Person]
    if(p.gt(x,y)) x else y
  }
  def main(args: Array[String]): Unit = {
    val zhang = new Person("zhang",23)
    val wang = new Person("wang",53)
    val ren = new Person("ren",13)
    val li = new Person("li",19)
    val han = new Person("han",56)
    var tmp:Person = zhang
    val persons = List(zhang,han,wang,ren,li)

    for(person <- persons){

      tmp = comparTwoP(tmp,person)
    }
    println(tmp)
  }
}

object GAOFUNCTION{
  def main(args: Array[String]): Unit = {
    val fu = (a:Int,b:Int) => (c:Int) => a+b+c
    val i: Int = fu(2,3)(4)
    println(i)
    val intToInt: Int => Int = fu(4,6)
    println(intToInt(3))

    val fun1 = (func:(Int,Int) => Int) => func
    val i1: Int = fun1(add)(4,5)
    println(i1)


    val ints = List(1,2,3,4,5)
    val ints1: List[Int] = ints.map(f => f*f)
    println(ints1)
  }

  def add(a:Int,b:Int) : Int = a+b
}