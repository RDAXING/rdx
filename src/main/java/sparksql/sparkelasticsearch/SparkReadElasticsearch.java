package sparksql.sparkelasticsearch;
import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.JavaEsRDD;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkReadElasticsearch  implements Serializable{
    /**
     * #esName = shunyi-cupid
     #esIp = 192.168.35.34,192.168.35.33,192.168.35.53
     #esPort = 9300
     */
    @Test
    public void SparkElasticSearch(){
        /**
         * 获取elasticsearch配置文件
         */
        SparkConf conf = new SparkConf().setAppName("SparkElasticSearch").setMaster("local[*]");
        conf.set("cluster.name", "Apollo-CBD");
        conf.set("es.read.metadata", "true");
//        conf.set("es.index.auto.create","true");
        conf.set("es.nodes", "121.52.212.147");
        conf.set("es.port", "9200");
        conf.set("es.nodes.wan.only", "true");
        conf.set("es.read.field.exclude", "_index,_type,_id,_score");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> esRDD = sqlContext.read().format("org.elasticsearch.spark.sql").load("company_dcrz_diyajc");
        esRDD.registerTempTable("user");
        Dataset<Row> sqlRdd = sqlContext.sql("select * from user limit 1");
//        Dataset<Row> sqlRdd = sqlContext.sql("select pname,body,miaoshu from user");
/**
 * //body|crawlTime|createTime|dataType|eventLevel|foreignId|ggbh|gslx|lanmuId|lastUpdateDate|menuName|miaoshu|
 moneyKind|party|pgmoney|pname|postTime|sortTime|sqmoney|src|srcUrl|status|subclass|syncStatus|title|zqxx|zytime|
 _metadata|
 */
//        sqlRdd.coalesce(1).write().format("csv").save("C:\\Users\\admin\\Desktop\\sparktest\\elastic");
        String sss = "body|crawlTime|createTime|dataType|eventLevel|foreignId|ggbh|gslx|lanmuId|lastUpdateDate|menuName|miaoshu|moneyKind|party|pgmoney|pname|postTime|sortTime|sqmoney|src|srcUrl|status|subclass|syncStatus|title|zqxx|zytime";

        String[] split = sss.split("\\|");

        Broadcast<String[]> broadcast = sc.broadcast(split);

        sqlRdd.show();
        JavaRDD<Row> javaRDD = sqlRdd.toJavaRDD();
        JavaRDD<Row> mapRdd = javaRDD.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String[] value = broadcast.value();
                int metadata = row.fieldIndex("_metadata");
                List<String> list = new ArrayList<>();
                String s = "";
                for (int i = 0; i < row.length(); i++) {
                    if (i == metadata) {
                        continue;
                    }
                    Object o = row.get(i);
                    if (null == o) {
                        o = "null";
                    }
                    s = o.toString().replaceAll("\n", " ");

                    list.add(s);
                }
//                String join = StringUtils.join(list, "==").replaceAll(",","，").replaceAll("==",",");
//
                for(int i=0;i<list.size();i++){
                    String v = list.get(i);
                    String k = value[i];
                    System.err.println(k + "::::" + v);

                }
                return RowFactory.create(list);
            }
        });

        mapRdd.collect();
        List<StructField> structFields = new ArrayList<>();
        System.err.println(split.length);
        for(int i=0;i<split.length;i++){
            String s = split[i];
            if(s.equals("crawlTime") || s.equals("createTime") || s.equals("postTime")||s.equals("sortTime")){
                structFields.add(DataTypes.createStructField(s,DataTypes.LongType,true));

            }else
            if(s.equals("status")){

                structFields.add(DataTypes.createStructField(s,DataTypes.IntegerType,true));
            }else
            structFields.add(DataTypes.createStructField(s,DataTypes.StringType,true));
        }

        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(mapRdd, structType);

        dataFrame.registerTempTable("person");
        sqlContext.sql("select * from person").show();

//
    }
    @Test
    public void test1(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("SPARK ES");
        conf.set("cluster.name", "Apollo-CBD");
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes", "121.52.212.147");
        conf.set("es.port", "9200");
        conf.set("es.nodes.wan.only", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");


        JavaRDD<String> values = JavaEsSpark.esJsonRDD(sc, "company_qcc/company_qcc").values();
        System.out.println(values);


//        for(Tuple2 tuple:esRDD.collect()){
//            System.out.print(tuple._1()+"----------");
//            System.out.println(tuple._2());
//        }
    }


    @Test
    public void sparkReadES(){

        SparkConf conf = new SparkConf().setAppName("SparkElasticSearch").setMaster("local[*]");
        conf.set("cluster.name", "Apollo-CBD");
        conf.set("es.nodes", "121.52.212.174,121.52.212.175,121.52.212.176");
        conf.set("es.port", "9200");
        conf.set("es.nodes.wan.only", "true");
        conf.set("es.index.auto.create", "true");
        conf.set("es.read.field.exclude", "_index,_type,_id,_score");
        conf.set("es.index.read.missing.as.empty","true");
//        conf.set(ConfigurationOptions.ES_QUERY, ajkQuery);
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        JavaRDD<Map<String, Object>> values = esRDD(jsc, "amr/amr").values();
        System.out.println(values.collect().size());

//        JavaPairRDD<String, Map<String, Object>> testindex = JavaEsSpark.esRDD(jsc, "bgtstd/bgtstd");
//        System.out.println(testindex.count());


//        JavaSparkContext jsc = new JavaSparkContext(conf);
//        JavaRDD<Map<String, Object>> searchRdd = esRDD(jsc, "testindex").values();
//        for (Map<String, Object> item : searchRdd.collect()) {
//            item.forEach((key, value)->{
//                System.out.println("search key:" + key + ", search value:" + value);
//            });
//        }



    }
    @Test
    public void test11(){

    }
}
