package sparksql.sparksql_UDF;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkUDF2 implements Serializable {

    @Test
    public void SparkUDF(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("udf");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> dataRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\udf.txt");
        JavaRDD<Row> rowJavaRDD = dataRdd.mapPartitions(new FlatMapFunction<Iterator<String>, Row>() {
            @Override
            public Iterator<Row> call(Iterator<String> stringIterator) throws Exception {
                List<Row> rows = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String value = stringIterator.next();
                    String[] split = value.split(",");
                    String name = split[0];
                    Integer age = Integer.parseInt(split[1]);
                    String address = split[2];
                    Row row = RowFactory.create(name, age, address);
                    rows.add(row);

                }

                return rows.iterator();
            }
        });

        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("address",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowJavaRDD, structType);

        //udf函数的创建
        sqlContext.udf().register("toUpper",(String f) -> f.toUpperCase(),DataTypes.StringType);
        sqlContext.udf().register("isNUll",(String field,String defultValue) ->field == null ||  field .equals(" ")  ? defultValue : field,DataTypes.StringType);

        dataFrame.registerTempTable("user");
        sqlContext.sql("select name,toUpper(name) ,isNUll(address,\"null\") from user").show();


        //udaf
        sqlContext.udf().register("concatAddress", new UserDefinedAggregateFunction() {
            @Override
            public StructType inputSchema() {
                List<StructField> address = Arrays.asList(DataTypes.createStructField("address", DataTypes.StringType, true));
                StructType structType1 = DataTypes.createStructType(address);
                return structType1;
            }

            @Override
            public DataType dataType() {
                return  DataTypes.StringType;
            }

            @Override
            public boolean deterministic() {
                return true;
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                String cityinfo = buffer.getString(0);
                String inCity = input.getString(0);
                if(!cityinfo.contains(inCity)){
                    if("".equals(cityinfo)){
                        cityinfo += inCity;
                    }else{
                        cityinfo += "," +inCity;
                    }
                    buffer.update(0,cityinfo);
                }
            }

            @Override
            public StructType bufferSchema() {
                List<StructField> address = Arrays.asList(DataTypes.createStructField("address", DataTypes.StringType, true));
                StructType structType1 = DataTypes.createStructType(address);
                return structType1;
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                String allCity = buffer1.getString(0);
                String otherCity = buffer2.getString(0);
                for(String cityinfo : otherCity.split(",")){
                    if(!allCity.contains(cityinfo)){
                        if("".equals(allCity)){
                            allCity += cityinfo;
                        }else{
                            allCity += "," + cityinfo;
                        }
                    }
                }
                buffer1.update(0,allCity);
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0,"");
            }

            @Override
            public Object evaluate(Row buffer) {
                return buffer.toString();
            }
        });


        sqlContext.sql("select name, concatAddress(address) from user group by name").show();


        sqlContext.udf().register("mysum", new UserDefinedAggregateFunction() {
            @Override
            public StructType inputSchema() {
                List<StructField> address = Arrays.asList(DataTypes.createStructField("age", DataTypes.IntegerType, true));
                StructType structType1 = DataTypes.createStructType(address);
                return structType1;
            }

            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }

            @Override
            public boolean deterministic() {
                return true;
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                int num = buffer.getInt(0);
                int othernum = input.getInt(0);
                if(num ==0){
                    num += othernum;
                }else{
                    num += othernum;
                }
                buffer.update(0,num);
            }

            @Override
            public StructType bufferSchema() {
                List<StructField> address = Arrays.asList(DataTypes.createStructField("address", DataTypes.IntegerType, true));
                StructType structType1 = DataTypes.createStructType(address);
                return structType1;
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                int resnum = buffer1.getInt(0);
                int othernum = buffer2.getInt(0);
                if (resnum == 0 )
                    resnum += othernum;
                else
                    resnum += othernum;
                buffer1.update(0,resnum);
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0,0);
            }

            @Override
            public Object evaluate(Row buffer) {
                return buffer.getInt(0);
            }
        });

        sqlContext.sql("select name,count(1) namecount,mysum(age) sumage ,avg(age) myavg, concatAddress(address) from user group by name").coalesce(1).write().format("csv").save("C:\\Users\\admin\\Desktop\\sparktest\\lallalalalallalalall");
    }
}
