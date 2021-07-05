package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

public class SparkSqlTest  implements Serializable{
    @Test
    public void SparkReadJson(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkJson");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inpath = "C:\\Users\\admin\\Desktop\\sparktest\\b.json";
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> jsonRdd = sqlContext.read().json(inpath);
        jsonRdd.registerTempTable("user");
        sqlContext.sql("select * from user").show();
        sqlContext.sql("select name,age,sex from user where age >= 20 and sex = \"f\"").show();
    }

    @Test
    public void SparkGlobalSession(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkGlobalSession").config(new SparkConf()).getOrCreate();
        Dataset<Row> jsonDS = spark.read().json("C:\\Users\\admin\\Desktop\\sparktest\\b.json");
        jsonDS.createOrReplaceGlobalTempView("user");
        jsonDS.sqlContext().sql("select name ,age ,sex from global_temp.user").show();
        spark.sql("select * from global_temp.user").show();
        spark.newSession().sql("select name,age from global_temp.user").show();
    }


    @Test
    public void SparkReadCSV(){
        SparkConf conf = new SparkConf().setAppName("csv").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        String inpath = "C:\\Users\\admin\\Desktop\\sparktest\\新建文件夹";
        Dataset<Row> csvRdd = sqlContext.read().csv(inpath).toDF("name", "age", "sex", "address");
        csvRdd.registerTempTable("user");
        sqlContext.sql("select * from user").show();

    }

    @Test
    public void  SparkReadMySql(){
        SparkConf conf = new SparkConf().setAppName("SparkReadMySql").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Map<String,String> map = new HashMap<>();
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        map.put("user","root");
        map.put("password","");
        map.put("dbtable","datataskinfor_bak");

        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> jdbcDF = sqlContext.read().format("jdbc").options(map).load();
        jdbcDF.registerTempTable("info");
//        sqlContext.sql("select searchTargetList from info where dataTaskId = 380").show();
        JavaRDD<Row> rowJavaRDD = sqlContext.sql("select searchTargetList from info where dataTaskId = 380").toJavaRDD();
        JavaRDD<List<String>> mapRdd = rowJavaRDD.map(new Function<Row, List<String>>() {
            @Override
            public List<String> call(Row row) throws Exception {
                String[] split = row.toString().replaceAll("\\[","").replaceAll("]","").split(",");
                List<String> list = Arrays.asList(split);
                return list;
            }
        });

        mapRdd.foreach(f ->{

            for(String name : f){

                System.err.println(name);
            }
        });

    }


    @Test
    public void SparkMysqlToJSon(){
        SparkConf conf = new SparkConf().setAppName("SparkReadMySql").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Map<String,String> map = new HashMap<>();
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        map.put("user","root");
        map.put("password","");
        map.put("dbtable","datataskinfor_bak");

        Properties writeJdbc = new Properties();
        writeJdbc.setProperty("driver","com.mysql.jdbc.Driver");
        writeJdbc.setProperty("url","jdbc:mysql://192.168.35.37:3306/pajt_zmn_datatest?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        writeJdbc.setProperty("user","root");
        writeJdbc.setProperty("password","/Akk2pyqQ8oh/hnjXZqZrp2d7X8BNk1:");
        writeJdbc.setProperty("dbtable","datataskinfor_bak");


        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> jdbcDF = sqlContext.read().format("jdbc").options(map).load();
        jdbcDF.registerTempTable("user");
        sqlContext.sql("select * from user where  dataTaskId >= 413").show();
//                .write().mode(SaveMode.Append).jdbc(writeJdbc.getProperty("url"),writeJdbc.getProperty("dbtable"),writeJdbc);
////                .write().format("json").save("C:\\Users\\admin\\Desktop\\sparktest\\json");
    }



    @Test
    public void SparkRDDDynamicDataFrame(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = new ArrayList<>();
        list.add("java,23,us");
        list.add("spark,45,en");
        list.add("hbase,36,cn");
        list.add("scala,88,ru");
        JavaRDD<String> rdd = sc.parallelize(list);
        JavaRDD<Row> mapRdd = rdd.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] split = s.split(",");
                String name = split[0];
                String count = split[1];
                String country = split[2];
                Row row = RowFactory.create(name, count, country);
                return row;
            }
        });

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("count",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("country",DataTypes.StringType,true));

        StructType structType = DataTypes.createStructType(structFields);

        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(mapRdd, structType);
        dataFrame.registerTempTable("user");
        sqlContext.sql("select * from user").show();;

    }
}
