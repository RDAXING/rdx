package sparkStreaming.dataTomysql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class SparkStreamingReadFileToMysql implements Serializable{
    public static void main(String[] args) {
        String checkpath = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\hello";
        Logger.getLogger("org").setLevel(Level.WARN);
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpath, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                SparkConf conf = new SparkConf().setAppName("SparkStreamingReadFileToMysql").setMaster("local[*]");
                conf.set("spark.driver.allowMultipleContexts","true");
                JavaSparkContext sc = new JavaSparkContext(conf);
                SQLContext sqlContext = new SQLContext(sc);
                JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(3));
                jsc.checkpoint(checkpath);
//                JavaDStream<String> lines = jsc.textFileStream("C:\\Users\\admin\\Desktop\\sparktest\\hadoopdir");
                JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 8989);
                lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                        List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                        for(String name : s.split(" ")){
                            tuple2s.add(new Tuple2<>(name,1));
                        }
                        return tuple2s.iterator();
                    }
                }).reduceByKey((f1,f2) ->{
                    return  f1 + f2;
                }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                    @Override
                    public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                        Integer sum = 0;
                        if(v2.isPresent()){
                            sum = v2.get();
                        }

                        for(Integer i : v1){
                            sum += i;
                        }
                        return Optional.of(sum);
                    }
                }).foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
                    @Override
                    public void call(JavaPairRDD<String, Integer> dataRdd) throws Exception {
                        if(!dataRdd.isEmpty()){
                            List<StructField> structFields = new ArrayList<>();
                            structFields.add(DataTypes.createStructField("word",DataTypes.StringType,true));
                            structFields.add(DataTypes.createStructField("count",DataTypes.IntegerType,true));
                            StructType structType = DataTypes.createStructType(structFields);
                            JavaRDD<Row> rowJavaRDD = dataRdd.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Row>() {
                                @Override
                                public Iterator<Row> call(Iterator<Tuple2<String, Integer>> v1) throws Exception {
                                    List<Row> rows = new ArrayList<>();
                                    while (v1.hasNext()) {
                                        Tuple2<String, Integer> next = v1.next();
                                        String name = next._1;
                                        Integer integer = next._2;
                                        Row row = RowFactory.create(name, integer);
                                        rows.add(row);
                                    }
                                    return rows.iterator();
                                }
                            });

                            Dataset<Row> dataFrame = sqlContext.createDataFrame(rowJavaRDD, structType);
                            //将数据插入到MySQL中

                            Properties writeJdbc = new Properties();
                            writeJdbc.setProperty("driver","com.mysql.jdbc.Driver");
                            writeJdbc.setProperty("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
                            writeJdbc.setProperty("user","root");
                            writeJdbc.setProperty("password","");
                            writeJdbc.setProperty("dbtable","wordcount");
                            dataFrame.write()
                                    .mode(SaveMode.ErrorIfExists)
                                    .jdbc(writeJdbc.getProperty("url"),writeJdbc.getProperty("dbtable"),writeJdbc);
                        }else{
                            System.out.println("没有进入数据");
                        }
                    }
                });
                return jsc;
            }
        });

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
