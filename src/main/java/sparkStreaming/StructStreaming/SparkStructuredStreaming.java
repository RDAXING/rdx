package sparkStreaming.StructStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkStructuredStreaming implements Serializable {
    static {
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    @Test
    public void structDemo1(){
        String checkpoint = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\structuredstreaming";
        SparkSession spark = SparkSession.builder().master("local[*]").appName("").config(new SparkConf()).getOrCreate();
        Dataset<Row> lines = spark.readStream().format("socket").option("host", "127.0.0.1").option("port", "8989").load();
        Dataset<String> stringDataset = lines.as(Encoders.STRING()).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] value = s.split(" ");
                List<String> list = Arrays.asList(value);

                return list.iterator();
            }
        }, Encoders.STRING());


        Dataset<Row> value = stringDataset.groupBy("value").count();

        StreamingQuery start = value.writeStream().queryName("user").outputMode("complete").trigger(Trigger.ProcessingTime(3000)).option("checkpointLocation", checkpoint).format("console").start();
//        spark.sql("select * from user").show();
        try {
            start.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void StructuredStreamingReadSocket(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("StructuredStreamingReadSocket").config(new SparkConf()).getOrCreate();
//        Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", "9090").load();
        Dataset<Row> lines = spark.readStream().format("socket").option("host", "127.0.0.1").option("port", "7887").load();

        Dataset<String> dataDF = lines.as(Encoders.STRING()).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                List<String> list = Arrays.asList(split);
                return list.iterator();
            }
        }, Encoders.STRING());

        Dataset<Row> value = dataDF.groupBy("value").count();


        StreamingQuery start = value.writeStream().outputMode("complete").trigger(Trigger.ProcessingTime(2000)).format("console").start();
        try {
            start.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }






    /**
     * structuredStreaming读取csv文件
     */
    @Test
    public void StructuredStreamingReadCSV(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("StructuredStreamingReadCSV").getOrCreate();
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("sex",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structSchema = DataTypes.createStructType(structFields);
        Dataset<Row> csvDataFrame = spark.readStream().schema(structSchema).csv("C:\\Users\\admin\\Desktop\\sparktest\\user");
        csvDataFrame.createOrReplaceTempView("user");
        Dataset<Row> result = spark.sql("select * from user");
//        Dataset<Row> result = spark.sql("select sex,avg(age) from user group by sex");
        StreamingQuery start = result.writeStream().outputMode("append").trigger(Trigger.ProcessingTime(0)).format("console").start();
        try {
            start.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void StructuredCsv(){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("").config(new SparkConf()).getOrCreate();
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> csv = spark.readStream().format("csv").schema(structType).load("C:\\Users\\admin\\Desktop\\sparktest\\user");
        StreamingQuery start = csv.writeStream().outputMode("append").trigger(Trigger.ProcessingTime(0)).format("console").start();
        try {
            start.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }

    /**
     * StructuredStreaming根据文件夹的格式自动创建分区
     * 格式：key=value
     * C:\\Users\\admin\\Desktop\\sparktest\\user\\month=11
     * C:\\Users\\admin\\Desktop\\sparktest\\user\\month=10
     */
    @Test
    public void SparkAutoPartations(){

        SparkSession spark = SparkSession.builder().appName("").master("local[*]").config(new SparkConf()).getOrCreate();
        StructType structType = DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("name", DataTypes.StringType, true),
                        DataTypes.createStructField("sex", DataTypes.StringType, true),
                        DataTypes.createStructField("age", DataTypes.IntegerType, true)
                )
        );
        Dataset<Row> csvDS = spark.readStream().schema(structType).csv("C:\\Users\\admin\\Desktop\\sparktest\\user");
        StreamingQuery start = csvDS.writeStream().format("console").outputMode("append").trigger(Trigger.ProcessingTime(0)).start();
        try {
            start.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}









