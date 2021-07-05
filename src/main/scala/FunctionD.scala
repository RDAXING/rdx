import org.apache.commons.lang3.StringUtils
import scala.beans.BeanProperty
object FunctionD {
  def main(args: Array[String]): Unit = {
      val a = 5
      val  b = 2
    val i = getRes(a,b,"+")
    println(i)
    val fb = fbn(7)
    println(fb)
  }
  def getRes(a:Int,b:Int,op:String):Int={
    if(op == "+"){
      a+b
    }else if(op == "-"){
      a-b
    }else{
      0
    }
  }

  def fbn(a:Int):Int={
    if(a==1 || a==2){
      1
    }else {
      fbn(a - 1) + fbn(a - 2)
    }
  }
}

object demo1{
  def main(args: Array[String]): Unit = {
    val tiger = new tiger()
    val str = tesr(tiger)
    println(str)
  }

  def tesr(tiger : tiger):String={
    tiger.name = "javak"
    tiger.name
  }
}

class tiger{
  var name = ""
}

object demo22{
  def main(args: Array[String]): Unit = {
    def hahah():Unit={
      println("hahah")
    }
    print("omk")
    hahah()

    val str = sya("haeel")
    println(str)
  }
  def sya(ok:String):String ={
    s"halll pok ${ok}"
  }


}

object  demo33{
  def main(args: Array[String]): Unit = {
    val str = say()
    println(str)
    val str1 = say("lalalla")
    println(str1)
  }

  def say(a:String = "hello"):String={
    a
  }
}

object demo44{
  def main(args: Array[String]): Unit = {
    val i = sum(1,2,3,45)
    println(i)

    val two = sum2(1,2,3,4,5)
    println(two)
  }
  def sum(a:Int*):Int={
    var sum = 0
    for(i <- a){
      sum += i
    }
    sum
  }
  def sum2(a:Int,args:Int*):Int={
    var sum = 0
    for(i <- args){
      sum += i
    }

    sum += a
    sum
  }
}

object lazydemo{
  def main(args: Array[String]): Unit = {
    lazy val res = lafu(1,2,3,4)
    println("))))))))))))))))))))))))")
    println(res)
  }
  def lafu(args:Int*):Int={
    var sum = 0;
    for(i <- args){
      sum += i
    }
    println("sum:"+sum)
    sum
  }
}
/**
  * 函数
 */
object demoFun{
  def main(args: Array[String]): Unit = {
    val res =(a:Int,b:Int) => {
      a+b
    }
    println(res(lafu(1,2,3),3))
    //函数作为方法的参数进行传入
    val i = lafu(res(4,5),1)
    println(i)
    //方法转函数
    def add(a:Int,b:Int):Int=a+b
    val intsToInt = add _
    println("方法转函数:"+intsToInt(4,5))
    //
    def funadd(func:(Int,Int)=>Int)=func(4,5)

    println( "===="+funadd(add))
  }

  def lafu(args:Int*):Int={
    var sum = 0;
    for(i <- args){
      sum += i
    }
    sum
  }
}


class Customer extends  Person {
  var name:String = _
  var age : Int = _
  var sex:Char = _
  var address : String = _

  override def toString = s"Customer($name, $age, $sex, $address)"+super.toString
}

class Person{
  var height:Double = _
  var weight: Int =_

  override def toString: String = s"$height,$weight"
}
object cado{
  def main(args: Array[String]): Unit = {
    val customer = new Customer
    customer.name="zhang"
    customer.age = 14
    customer.sex = 'f'
    customer.address = "china"
    print(customer)

    val calculator = new Calculator(3,4)
    val i = calculator.allresult()
    print(i)
  }
}

class Calculator(height:Int,weight:Int){
  var res = height*weight
  var longs = 2*height + 2*weight

  def allresult():Int=res+longs
}

class du(name:String ,age:Int){
  var na = name
  var ag = age
  def du(name:String) ={
    this.na=name
  }
}


class PP{
  private var name:String = _
  private[this] var address:String = _
  var sex : Char = _
  val op = "rdx"
  def setName(name:String):Unit=this.name = name
  def getName():String = this.name
  def setAddress(address:String): Unit = this.address = address
  def getAddress():String = this.address
  def setSex(sex:Char):Unit = this.sex= sex
  def getSex():Char = this.sex

  override def toString: String = s"$name , $address ,$sex , $op"
}


object PP{
  def main(args: Array[String]): Unit = {
    val pp = new PP

    pp.setName("ha")
    pp.setAddress("china")
    pp.setSex('M')
    print(pp.toString)
  }
}

object PPO{
  def main(args: Array[String]): Unit = {
    val pp = new PP
    pp.setName("java")
    print(pp.getName())
  }
}

class FU(var name:String){
  var sex:Char = _
  var address : String =_
  def this(name:String,address:String)={
    this(name)
    this.address=address
  }

  def this(name:String,sex:Char) = {
    this(name)
    this.sex = sex
  }



}

class ConTest2(var name:String) {

  var age:Int = _

  var sex:String = _
  // 定义辅助构造器的参数列表，不能和主构造器的参数列表相同，因为scala不知道用谁，编译器报错
  //  def this(name:String) = {
  //    this(name)
  //  }

  // 定义辅助构造器
  // 辅助构造器参数不能用 var val 修饰
  def this(name:String, age:Int) = {
    // 第一行掉主构造器
    this(name)
    // 当你要初始化除了主构造器的参数外的属性，需要主动的来定义该属性
    this.age = age
  }

  def this(name:String,sex:String)={
    this(name)
    this.sex = sex
  }

  override def toString: String = s"name:${name},age:${age}, sex:${sex}"
}

object ConTest2{
  def main(args: Array[String]): Unit = {
    // 用主构造器new对象
    val c = new ConTest2("hainiu")
    println(c)
    // 用辅助构造器new对象
    val c2 = new ConTest2("hainiu", 10)
    println(c2)

    val c3 = new ConTest2("hainiu", "sex")
    println(c3)
  }
}

class He{
  import scala.beans.BeanProperty
  @BeanProperty var name:String = _

}

object He{
  def main(args: Array[String]): Unit = {
    val he = new He
    he.setName("hahah")
    he.getName()
    print(he.name)
  }
}

/**
  * 定义抽象类
  */
abstract  class Car{
  //定义抽象属性
  var carName : String
  val name : String = "车"

  def describe():Unit

  def action():String = {
    "so much money"
  }
}

class BMWCar extends  Car{
  var carName : String = "BMO"

  override def describe(): Unit = {

    print(carName+name+action())
  }
}

object domin{
  def main(args: Array[String]): Unit = {
    val car = new BMWCar
    car.describe()

    val car1 = new BCCar
    car1.describe()


    val tVar = new BTVar("haha","hongse")
    tVar.name = "bengtian"
    println(tVar.describe())
  }
}

/**
  * 定义一个抽象类
  */

abstract class BaseCa{
  val name : String
  val struct : String = "four cycle"
  def describe():Unit
  def action():String = {
    name + "so beautiful"
  }
}


class BCCar extends  BaseCa{
  val name : String = "ben chi"

  override def describe(): Unit = {
    print(name+"so quick " + struct + action())
  }
}


abstract  class Base(types:String){
  var name:String
  val option:String = "四驱"
  def describe():Unit
  def action(money:Double):String={
    name + "is" + option + "and money is " + money
  }
}

class BTVar(types:String,other:String) extends  Base(types:String){
  override var name: String = _
  override def describe(): Unit = {
    types + other + action(35.5)
  }
}



abstract class Car2(val name:String) {
  var types:String = _


  def this(name:String, types:String)={
    this(name)
    this.types = types
  }
}
// 定义子类的构造器继承父类的辅助构造器
class BMWCar2(name:String, types:String, brand:String) extends Car2(name:String, types:String){
  override def toString: String = s"name:${name}, types:${types}, brand:${brand}"
}

object BMWCar2{
  def main(args: Array[String]): Unit = {
    val car = new BMWCar2("车", "X7", "宝马")
    println(car)
  }
}


/**
  * 特质的使用
  */
trait FLy{
  var name:String
  val ff:String = "飞"
  def describute():Unit
  def add(a:Int,b:Int):Int={
    a+b
  }
}

class Bird(a:Int ,b:Int) extends  FLy{
  var name:String = _
  override def describute(): Unit = {
    print(ff+name+add(a,b))
  }
}

object m{
  def main(args: Array[String]): Unit = {
    val bird = new Bird(1,2)
    bird.name = "cpws"
    bird.describute()
  }
}








