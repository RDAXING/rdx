import scala.io.Source

object ImplicitDe {
  //定义定义一个隐shi函数
  implicit val implicitF = (a:Int) =>{
    println("implicitF")
    a.toString
  }

  //定义隐shi方法
  implicit def implicitM(a:Int) :String ={
    println("implicitM")
    a.toString
  }
  def main(args: Array[String]): Unit = {
    // 当有隐式函数时，会优先使用隐式函数，这体现了函数是一等公民
    // 如果没有隐式函数，就找隐式的方法
    val i = 1234
    println(i.length) //implicitF 4

  }
}

class ImplicitC(path:String){
  def read():String = {
    val files: String = Source.fromFile(path).mkString
    files
  }
}

object ImplicitC{
  def main(args: Array[String]): Unit = {
    val  path:String = "C:\\Users\\admin\\Desktop\\sparktest\\a.txt";
    val c = new ImplicitC(path)
    val str: String = c.read()
    println(str)
  }
}