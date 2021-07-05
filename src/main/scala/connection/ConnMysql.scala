package connection
import java.sql.{Connection, DriverManager, ResultSet, Statement}

object ConnMysql {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://localhost:3306/testdata"
    val username = "root"
    val password = ""
    var connection:Connection = null
    classOf[com.mysql.jdbc.Driver]
    try {
      connection = DriverManager.getConnection(url, username, password)
      val statement: Statement = connection.createStatement()
      val result: ResultSet = statement.executeQuery("select * from offset")
      while (result.next()) {
        val group: String = result.getString(1)
        val topic: String = result.getString(2)
        val partition: String = result.getString(3)
        val offset: String = result.getString(4)
        val timestamp: String = result.getString(5)
        println(s"${group},${topic},${partition},${offset},${timestamp}")
      }
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      connection.close()
    }


  }
}



object MySQLApp {

  def main(args: Array[String]): Unit = {
    val url ="jdbc:mysql://localhost:3306/testdata"
    val username = "root"
    val password = ""
    var connection:Connection = null
    try{
      //创建连接
      classOf[com.mysql.jdbc.Driver]
      connection = DriverManager.getConnection(url,username,password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select * from offset")
      while (resultSet.next()){
        val id = resultSet.getString("consumer_group")
        val name = resultSet.getString("sub_topic_partition_offset")

        println(s"$id , $name")
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
        connection.close()
    }
  }
}

