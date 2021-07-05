package mysqlutil

object MyPredef {

  implicit object STUCOM extends  Ordering[HNS]{
    override def compare(x: HNS, y: HNS): Int = {
      println("比较年龄")
      x.age-y.age
    };
  }

  implicit val implicitD2ReadFile=(path:String) =>{
    //
    println("隐藏函数")
    new ImplicitD2(path)
  }

  //定义一个隐式对象
  implicit object ImplicitObj{
    def sayH(str:String):String = {
      s"hah:${str}"
    }
  }

  implicit val rdFile = (path : String) =>{
    println("scala")
    new RD(path)
  }


  implicit object PersonCom extends Ordering[Person]{
    override def compare(x: Person, y: Person): Int = {
      x.age-y.age
    }
  }
}






