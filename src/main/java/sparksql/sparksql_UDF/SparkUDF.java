package sparksql.sparksql_UDF;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkUDF implements Serializable {
    /**
     * sparksql --------udf的使用
     * 1.将某个字段的小写转化为大写
     * sqlContext.udf().register("toUpper",(String field) ->field.toUpperCase(), DataTypes.StringType);
     * 2.判断某个字段是否为空，如果为空用指定的字符进行代替
     * sqlContext.udf().register("isNull",(String field,String defualvalue) ->field==null || field.equals(" ") ? defualvalue : field,DataTypes.StringType);
     */
    @Test
    public void SparkUDF1(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("udf");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        sqlContext.udf().register("toUpper",(String field) ->field.toUpperCase(), DataTypes.StringType);
        sqlContext.udf().register("isNull",(String field,String defualvalue) ->field==null || field.equals(" ") ? defualvalue : field,DataTypes.StringType);
        JavaRDD<String> dataRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\udf.txt");
        JavaRDD<Row> mapRdd = dataRdd.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {

                return RowFactory.create(s.split(","));
            }
        });

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("address",DataTypes.StringType,true));

        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(mapRdd, structType);
        dataFrame.registerTempTable("user");
        sqlContext.sql("select toUpper(name),age,isNull(address,\"null\") from user").show();
    }

    @Test
    public void SparkUDAF(){
        SparkConf conf = new SparkConf().setAppName("udaf").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> dataRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\udf.txt");
        JavaRDD<Row> mapRdd = dataRdd.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] split = s.split(",");
                String name = split[0];
                Integer age = Integer.parseInt(split[1]);
                String address = split[2];
                return RowFactory.create(name,age,address);
            }
        });

        //rdd---->dataframe
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("address",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> df = sqlContext.createDataFrame(mapRdd, structType);
        df.registerTempTable("user");


        /**
         * 统计某个属性出现的数值相当于sum/count
         */
        sqlContext.udf().register("StringCount", new UserDefinedAggregateFunction() {
            @Override
            public StructType inputSchema() {
                List<StructField> structFields1 = new ArrayList<>();
                structFields1.add(DataTypes.createStructField("name",DataTypes.StringType,true));

                return DataTypes.createStructType(structFields1);
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
                buffer.update(0,buffer.getInt(0)+1);
            }

            @Override
            public StructType bufferSchema() {
                List<StructField> df1 = Arrays.asList(DataTypes.createStructField("df", DataTypes.IntegerType, true));
                StructType structType1 = DataTypes.createStructType(df1);
                return structType1;
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
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

        /**
         * 先分组在统计出现的次数
         */
        sqlContext.sql("select name,StringCount(name) from user group by name").show();
        sqlContext.sql("select name,count(1) from user group by name").show();
        /**
         * 自定义求平均值
         */
        sqlContext.udf().register("myavg", new UserDefinedAggregateFunction() {
            @Override
            public StructType inputSchema() {
                List<StructField> structFields1 = new ArrayList<>();
                structFields1.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
                StructType structType1 = DataTypes.createStructType(structFields1);
                return structType1;
            }

            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }

            @Override
            public boolean deterministic() {
                return false;
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                         buffer.update(0,buffer.getInt(0)+1);
                         buffer.update(1,buffer.getInt(1)+input.getInt(0));

            }

            @Override
            public StructType bufferSchema() {
                List<StructField> structFields1 = new ArrayList<>();
                structFields1.add(DataTypes.createStructField("field1",DataTypes.IntegerType,true));
                structFields1.add(DataTypes.createStructField("field2",DataTypes.IntegerType,true));
                StructType structType1 = DataTypes.createStructType(structFields1);
                return structType1;
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
                buffer1.update(1,buffer1.getInt(1)+buffer2.getInt(1));
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0,0);
                buffer.update(1,0);
            }

            @Override
            public Object evaluate(Row buffer) {
                return buffer.getInt(1)/buffer.getInt(0);
            }
        });


        sqlContext.sql("select myavg(age) from user").show();
        sqlContext.sql("select name,StringCount(name),myavg(age) from user group by name").show();

        /**
         * 拼接地址信息
         */
        sqlContext.udf().register("unionAddress", new UserDefinedAggregateFunction() {
            @Override
            public StructType inputSchema() {
                List<StructField> address = Arrays.asList(DataTypes.createStructField("address", DataTypes.StringType, true));
                StructType structType1 = DataTypes.createStructType(address);
                return structType1;
            }

            @Override
            public DataType dataType() {
                return DataTypes.StringType;
            }

            @Override
            public boolean deterministic() {
                return true;
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                // 缓冲中的已经拼接过的城市信息串
                String bufferCityInfo = buffer.getString(0);
                // 刚刚传递进来的某个城市信息
                String cityInfo = input.getString(0);

                // 在这里要实现去重的逻辑
                // 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
                if (!bufferCityInfo.contains(cityInfo)) {
                    if ("".equals(bufferCityInfo)) {
                        bufferCityInfo += cityInfo;
                    } else {
                        // 比如1:北京
                        //2：上海
                        //结果 1:北京,2:上海
                        //再 来一个 1：北京  就不会拼接进去。
                        bufferCityInfo += "," + cityInfo;
                    }

                    buffer.update(0, bufferCityInfo);
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
                String totalCity = buffer1.getString(0);
                String otherCity = buffer2.getString(0);
                for(String city:otherCity.split(",")){
                    if(!totalCity.contains(city)){
                        if("".equals(totalCity)){
                            totalCity += city;
                        }else{
                            totalCity += ","+ city;
                        }
                    }
                }
                buffer1.update(0,totalCity);
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

//        sqlContext.sql("select name,StringCount(name),myavg(age),unionAddress(address) from user group by name").show();

    }

    @Test
    public void SparkSQLOfUDAF(){
        SparkConf conf = new SparkConf().setAppName("sqludaf").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> dataRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\udf.txt",1);
        sqlContext.udf().register("union", new UserDefinedAggregateFunction() {
            @Override
            public StructType inputSchema() {
                List<StructField> address = Arrays.asList(DataTypes.createStructField("address", DataTypes.StringType, true));
                StructType structType1 = DataTypes.createStructType(address);
                return structType1;
            }

            @Override
            public DataType dataType() {
                return DataTypes.StringType;
            }

            @Override
            public boolean deterministic() {
                return true;
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                String cityinfo = buffer.getString(0);
                String inputCity = input.getString(0);
                if(!cityinfo.contains(inputCity)){
                    if("".equals(cityinfo)){
                        cityinfo += inputCity;
                    }else{
                        cityinfo += ","+inputCity;
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
                String totalCity = buffer1.getString(0);
                String otherCity = buffer2.getString(0);
                for(String city : otherCity.split(",")){
                    if(!totalCity.contains(city)){
                        if("".equals(totalCity)){
                            totalCity += city;
                        }else{
                            totalCity += "," + city;
                        }
                    }
                }
                buffer1.update(0,totalCity);
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
        JavaRDD<Row> rowRdd = dataRdd.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] split = s.split(",");
                String name = split[0];
                Integer age = Integer.parseInt(split[1]);
                String address = split[2];
                return RowFactory.create(name, age, address);
            }
        });

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("address",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowRdd, structType);
        dataFrame.registerTempTable("user");
//        sqlContext.sql("select * from user").show();

        Dataset<Row> sqlRdd = sqlContext.sql("select name, union(address) from user group by name");
        sqlRdd.show();
//        sqlRdd.coalesce(1).write().format("csv").save("C:\\Users\\admin\\Desktop\\sparktest\\sqlRdd");

    }

    @Test
    public void SparkSqlRow_Column(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Row_Column");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> jsonRdd = sqlContext.read().json("C:\\Users\\admin\\Desktop\\sparktest\\class.json");
        jsonRdd.registerTempTable("user");
        sqlContext.sql("select * from user").show();
        sc.stop();

    }
}
