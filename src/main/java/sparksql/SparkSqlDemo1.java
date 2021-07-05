package sparksql;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import scala.Tuple3;
import sparksql.selfClass.ClearData;
import sparksql.selfClass.SortByKyComparator;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class SparkSqlDemo1 implements Serializable {
//    private Logger log = Logger.getLogger(SparkSqlDemo1.class);
    /**
     * sparksql-------读取json数据
     */
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    @Test
    public void SparkSqlone(){
//        SparkConf conf = new SparkConf();
//        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkSqlone").config(conf).getOrCreate();
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext spark = new SQLContext(sc);
        String jsonpath = "C:\\Users\\admin\\Desktop\\sparktest\\readJson";
//        String jsonpath = "C:\\Users\\admin\\Desktop\\sparktest\\b.json";
        Dataset<Row> json = spark.read().json(jsonpath);

        json.createOrReplaceTempView("user");
        json.sqlContext().sql("select sex, count(1) from user group by sex").show();
        json.sqlContext().sql("select * from user order by age desc").show();
        json.sqlContext().sql("select * from user a where sex = \"m\"").show();
    }

    /**
     * 创建临时会话
     */
    @Test
    public void SparkSQLTwo(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkSQLTwo").getOrCreate();
        Dataset<Row> jsonDS = spark.read().json("C:\\Users\\admin\\Desktop\\sparktest\\b.json");
        jsonDS.createOrReplaceTempView("user");
        Dataset<Row> sqlDS = jsonDS.sqlContext().sql("select * from user");
        sqlDS.show();
        jsonDS.sqlContext().sql("select name,age,sex from user where age >=20 and age <=40 and sex = \"m\"").show();

    }

    /**
     * sparksql -------全局会话
     */
    @Test
    public void SparkSQLThree(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkSQLThree").config(new SparkConf()).getOrCreate();
        Dataset<Row> jsonDS = spark.read().json("C:\\Users\\admin\\Desktop\\sparktest\\b.json");
        jsonDS.createOrReplaceGlobalTempView("user");
        jsonDS.sqlContext().sql("select name ,age ,sex from global_temp.user").show();
        spark.sql("select * from global_temp.user").show();
        spark.newSession().sql("select name,age from global_temp.user").show();
    }

    /**
     * sparksql -------读取csv文件
     */
    @Test
    public void SparkReadCSV(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("csv").config(new SparkConf()).getOrCreate();
        String csvpath= "C:\\Users\\admin\\Desktop\\sparktest\\新建文件夹\\c.csv";
        Dataset<Row> csvDS = spark.read().csv(csvpath).toDF("name","age","sex","address");
        csvDS.createOrReplaceTempView("user");
//        spark.sql("select name from user").write().format("json").save("C:\\Users\\admin\\Desktop\\sparktest\\json");
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\orc";
        try {
            FileUtils.deleteDirectory(new File(path));
//            System.out.println("创建目录");
//            FileUtils.forceMkdir(new File(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        spark.sql("select * from user").write().orc("C:\\Users\\admin\\Desktop\\sparktest\\orc");

    }

    /**
     * spark读取MySQL中的数据
     */
    @Test
    public void SparkReadMysql(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("readMysql").config(new SparkConf()).getOrCreate();
        Map<String, String> map = new HashMap<>();
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        map.put("user","root");
        map.put("password","");
        map.put("dbtable","datataskinfor_bak");
        Dataset<Row> jdbcDS = spark.read().format("jdbc").options(map).load();
        jdbcDS.registerTempTable("user");
//        spark.sql("select searchTargetList from user where dataTaskId = 414").write().format("json").save("C:\\Users\\admin\\Desktop\\sparktest\\mysql");
        JavaRDD<Row> javaRDD = spark.sql("select searchTargetList from user where dataTaskId = 414").toJavaRDD();
        JavaRDD<List<String>> rddList = javaRDD.map(new Function<Row, List<String>>() {
            @Override
            public List<String> call(Row row) throws Exception {
                String[] split = row.toString().split(",");
                System.out.println(split.length);
                List<String> list = Arrays.asList(split);

                return list;
            }
        });
//        System.out.println(rddList.collect());
        rddList.foreach(new VoidFunction<List<String>>() {
            @Override
            public void call(List<String> strings) throws Exception {
                for(String name : strings){
                    System.out.println(name);
                }
            }
        });
    }

    @Test
    public void SparkSqlReadMysdqlDemo(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        SparkContext sc = SparkContext.getOrCreate(conf);
//        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
//        SQLContext sqlContext = new SQLContext(jsc);'

        SparkSession spark = SparkSession.builder().appName("").master("local[*]").config(conf).getOrCreate();
        Map<String, String> map = new HashMap<>();
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        map.put("user","root");
        map.put("password","");
        map.put("dbtable","datataskinfor_bak");
        Dataset<Row> jdbcDF = spark.read().format("jdbc").options(map).load();
        JavaRDD<Row> rowJavaRDD = jdbcDF.toJavaRDD();
        JavaRDD<String> resRdd = rowJavaRDD.distinct().mapPartitions(new FlatMapFunction<Iterator<Row>, String>() {
            @Override
            public Iterator<String> call(Iterator<Row> row) throws Exception {
                List<String> strings = new ArrayList<>();
                while (row.hasNext()) {
                    Row next = row.next();
                    String key = String.valueOf(next.get(0));
                    String value = next.getString(7);
                    String res = key + "\001" + value;
                    strings.add(res);
//                    System.out.println(res);
                }
                return strings.iterator();
            }
        });
        long count = resRdd.count();
        JavaPairRDD<Integer, List<String>> resRDD = resRdd.mapPartitionsToPair(
//            new PairFlatMapFunction<Iterator<String>, Integer, List<String>>() {
//            @Override
//            public Iterator<Tuple2<Integer, List<String>>> call(Iterator<String> row) throws Exception {
//                List<Tuple2<Integer, List<String>>> tuple2s = new ArrayList<>();
//                while (row.hasNext()) {
//                    String next = row.next();
//                    String[] split = next.split("\001");
//                    String s = split[1];
//                    String[] listArray = s.split(",");
//                    List<String> list = Arrays.asList(listArray);
//                    Tuple2<Integer, List<String>> tu = new Tuple2<>(Integer.parseInt(split[0]), list);
////                    System.out.println(tu);
//                    tuple2s.add(tu);
//                }
//                return tuple2s.iterator();
//            }
//        }
                new ClearData()
        );

        JavaPairRDD<Integer, List<String>> comRdd = resRDD.sortByKey(new SortByKyComparator(),false);
        comRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, List<String>>>>() {
            @Override
            public void call(Iterator<Tuple2<Integer, List<String>>> tuple2Iterator) throws Exception {
                while(tuple2Iterator.hasNext()){
                    System.err.println(tuple2Iterator.next());
                }
            }
        });
        System.out.println(resRDD.count());

    }

    /**
     * 读取MySQL数据并且写入数据到other的MySQL
     */
    @Test
    public void SparReadMysqlAndWriteMysql(){
//        log.info("程序开始运行");
//        log.info("创建sparksessoin");
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparReadMysqlAndWriteMysql")
                .config(new SparkConf())
                .getOrCreate();

        //获取MySQL的jdbs并且读取数据
//        log.info("获取本地MySQL的JDBC");
        Map<String, String> readJdbc = new HashMap<>();
        readJdbc.put("driver","com.mysql.jdbc.Driver");
        readJdbc.put("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        readJdbc.put("user","root");
        readJdbc.put("password","");
        readJdbc.put("dbtable","datataskinfor_bak");

        //获取写入的MySQL的jdbc
//        log.info("获取服务器上的JDBC");
        Properties writeJdbc = new Properties();
        writeJdbc.setProperty("driver","com.mysql.jdbc.Driver");
        writeJdbc.setProperty("url","jdbc:mysql://192.168.35.37:3306/pajt_zmn_datatest?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        writeJdbc.setProperty("user","root");
        writeJdbc.setProperty("password","/Akk2pyqQ8oh/hnjXZqZrp2d7X8BNk1:");
        writeJdbc.setProperty("dbtable","datataskinfor_bak");
        Dataset<Row> jdbcDS = spark.read().format("jdbc").options(readJdbc).load();

//        log.info("正在创建临时库");
        jdbcDS.registerTempTable("user");
        spark.sql("select * from user where dataTaskId >= 413")
                .write()
                .mode(SaveMode.Append)
                .jdbc(writeJdbc.getProperty("url"),writeJdbc.getProperty("dbtable"),writeJdbc);
//        log.info("写入数据成功");

    }


    /**
     * DataFrame,Rdd,DataSet相互相互转换
     * (1) RDD转换为DataFrame
     */
    @Test
    public void SparkRddToDataFrame(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkRddToDataFrame");
//        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
//                .appName("SparkRddToDataFrame")
//                .config(conf)
//                .getOrCreate();


        List<Tuple3> list = new ArrayList<>();
        list.add(new Tuple3<String,String,String>("spark","23","female"));
        list.add(new Tuple3<String,String,String>("java","12","male"));
        list.add(new Tuple3<String,String,String>("python","44","female"));
        list.add(new Tuple3<String,String,String>("scala","32","male"));
        list.add(new Tuple3<String,String,String>("hive","16","female"));
        list.add(new Tuple3<String,String,String>("hbase","36","male"));
        list.add(new Tuple3<String,String,String>("kafka","54","female"));

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Tuple3> dataRdd = sc.parallelize(list);

        JavaRDD<Person> personRdd = dataRdd.map(new Function<Tuple3, Person>() {
            @Override
            public Person call(Tuple3 tuple3) throws Exception {
                Person person = new Person();
                person.setName(String.valueOf(tuple3._1()));
                person.setAge(Integer.parseInt(String.valueOf(tuple3._2())));
                person.setSex(String.valueOf(tuple3._3()));
                return person;
            }
        });
        System.err.println(personRdd.collect());
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> df = sqlContext.createDataFrame(personRdd, Person.class);
        df.toDF().show();
        df.registerTempTable("person");
        sqlContext.sql("select name from person where age > 30").show();
    }

    /**
     * DataFrame 转换为RDD
     */
    @Test
    public void SparkSqlDataFrameToRDD(){
        SparkConf conf = new SparkConf().setAppName("SparkSqlDataFrameToRDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext spark = new SQLContext(sc);
        String path= "C:\\Users\\admin\\Desktop\\sparktest\\b.json";
        Dataset<Row> jsonRdd = spark.read().json(path);
        jsonRdd.registerTempTable("user");
        spark.sql("select * from user").show();
        JavaRDD<Row> rddRdd = jsonRdd.toJavaRDD();
        rddRdd.foreach(f ->{
            System.out.println(f);
        });
    }

    /**
     * RDD与DataFrame动态转换（Java）
     */
    @Test
    public void SparkRddToDataSet(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkRddToDataSet");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext spark = new SQLContext(sc);
        List<Tuple3> list = new ArrayList<>();
        list.add(new Tuple3<String,String,String>("spark","23","female"));
        list.add(new Tuple3<String,String,String>("java","12","male"));
        list.add(new Tuple3<String,String,String>("python","44","female"));
        list.add(new Tuple3<String,String,String>("scala","32","male"));
        list.add(new Tuple3<String,String,String>("hive","16","female"));
        list.add(new Tuple3<String,String,String>("hbase","36","male"));
        list.add(new Tuple3<String,String,String>("kafka","54","female"));
        JavaRDD<Tuple3> dataRdd = sc.parallelize(list);

        //首先，必须将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> mapRdd = dataRdd.map(new Function<Tuple3, Row>() {
            @Override
            public Row call(Tuple3 tuple3) throws Exception {
                String name = String.valueOf(tuple3._1());
                Integer age = Integer.parseInt(String.valueOf(tuple3._2()));
                String sex = String.valueOf(tuple3._3());

                return RowFactory.create(name,age,sex);
            }
        });

        /**
         * 第二步：动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型可能来自于
         * JSON文件，也可能来自于DB
         */

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("sex", DataTypes.StringType,true));

        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> df = spark.createDataFrame(mapRdd, structType);
        df.registerTempTable("users");
//        spark.sql("select * from users").show();
        Dataset<Row> changeRdd = spark.sql("select * from users where age >=30");
        /**
         * dataframe 转换为RDD
         */
        List<Row> collect = changeRdd.javaRDD().collect();
        System.out.println(collect);


    }

    @Test
    public void test(){
        List<Tuple3> list = new ArrayList<>();
        list.add(new Tuple3<String,String,String>("spark","23","female"));
        list.add(new Tuple3<String,String,String>("java","12","male"));
        list.add(new Tuple3<String,String,String>("python","44","female"));
        list.add(new Tuple3<String,String,String>("scala","32","male"));
        list.add(new Tuple3<String,String,String>("hive","16","female"));
        list.add(new Tuple3<String,String,String>("hbase","36","male"));
        list.add(new Tuple3<String,String,String>("kafka","54","female"));

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<Tuple3> dataRdd = sc.parallelize(list);
        JavaRDD<Row> rowRDD = dataRdd.mapPartitions(new FlatMapFunction<Iterator<Tuple3>, Row>() {
            @Override
            public Iterator<Row> call(Iterator<Tuple3> value) throws Exception {
                List<Row> rows = new ArrayList<>();
                while (value.hasNext()) {
                    Tuple3 next = value.next();
                    String name = String.valueOf(next._1());
                    Integer age = Integer.parseInt(String.valueOf(next._2()));
                    String sex = String.valueOf(next._3());
                    Row row = RowFactory.create(name, age, sex);
                    rows.add(row);
                }
                return rows.iterator();
            }
        });

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("sex",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowRDD, structType);
        dataFrame.show();
    }


    /**
     * spark自定义UDF的使用
     */
    public void SparkUDF(){


    }


    @Test
    public void sparkSqlPvUv(){
        Map<String, DataType> fieldMap = new HashMap<>();
        Logger.getLogger("org").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("pvuv").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> dataRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\pv_uv.txt");
        JavaRDD<Row> rowJavaRDD = dataRdd.mapPartitions(new FlatMapFunction<Iterator<String>, Row>() {
            @Override
            public Iterator<Row> call(Iterator<String> v1) throws Exception {
                List<Row> rows = new ArrayList<>();
                while (v1.hasNext()) {
                    String next = v1.next();
                    String[] split = next.split("\t");
                    String ip = split[0];
                    String address = split[1];
                    String date = split[2];
                    String timestamp = split[3];
                    String id = split[4];
                    String page = split[5];
                    String name = split[6];
                    Row row = RowFactory.create(ip, address, date, timestamp, id, page, name);
                    rows.add(row);
                }

                return rows.iterator();
            }
        });

        fieldMap.put("ip",DataTypes.StringType);
        fieldMap.put("address",DataTypes.StringType);
        fieldMap.put("date",DataTypes.StringType);
        fieldMap.put("timestamp",DataTypes.StringType);
        fieldMap.put("id",DataTypes.StringType);
        fieldMap.put("page",DataTypes.StringType);
        fieldMap.put("name",DataTypes.StringType);
//        List<StructField> structFields = new ArrayList<>();
//        structFields.add(DataTypes.createStructField("ip",DataTypes.StringType,true));
//        structFields.add(DataTypes.createStructField("address",DataTypes.StringType,true));
//        structFields.add(DataTypes.createStructField("date",DataTypes.StringType,true));
//        structFields.add(DataTypes.createStructField("timestamp",DataTypes.LongType,true));
//        structFields.add(DataTypes.createStructField("id",DataTypes.StringType,true));
//        structFields.add(DataTypes.createStructField("page",DataTypes.StringType,true));
//        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        StructType tableStructType = getTableStructType(fieldMap);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowJavaRDD, tableStructType);
        dataFrame.createOrReplaceTempView("user");
        sqlContext.sql("select distinct(ip || \"_\"||page) as lin,page from user").show();
        sqlContext.sql("select a.p,count(1) from (select distinct(ip || \"_\"||page) as lin,page as p from user) a group by a.p").show();
        sqlContext.sql("select page ,count(1) from user group by page").show();
        sqlContext.sql("select count(1) from (select distinct(ip || \"_\"||page) as lin from user) a").show();
        sqlContext.sql("select count(1) from user").show();
    }

    @Test
    public void SparkCoreTranformationSparkSql(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTranformationSparkSql");
        SparkContext sc = SparkContext.getOrCreate(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        JavaRDD<String> lines = jsc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\pv_uv.txt");
        lines.mapPartitions(new FlatMapFunction<Iterator<String>, Row>() {
            @Override
            public Iterator<Row> call(Iterator<String> value) throws Exception {
                List<Row> rows = new ArrayList<>();
                while(value.hasNext()){
                    String next = value.next();
                    String[] split = next.split("\t");
                    String ip = split[0];
                    String address = split[1];
                    String date = split[2];
                    String timestamp = split[3];
                    String id = split[4];
                    String page = split[5];
                    String option = split[6];
                    Row row = RowFactory.create(ip, address, date, timestamp, id, page, option);
                    rows.add(row);

                }
                return null;
            }
        });


    }

    public static StructType getTableStructType(Map<String,DataType> fieldmap){
        List<StructField> structFields = new ArrayList<>();
        for(Map.Entry<String,DataType> map : fieldmap.entrySet()) {
            String key = map.getKey();
            DataType value = map.getValue();
            System.out.println(key + ":"+value);
            structFields.add(DataTypes.createStructField(key,value,true));
        }
        StructType structType = DataTypes.createStructType(structFields);
        return structType;
    }
}
