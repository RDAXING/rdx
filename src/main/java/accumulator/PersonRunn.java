package accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class PersonRunn {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PersonRunn").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "zhangsan 1", "lisi 2", "wangwu 3", "zhaoliu 4", "tianqi 5", "zhengba 6","zhaoliu 4"
        ),3);
        PersonAccumulator acc = new PersonAccumulator();
        sc.sc().register(acc,"person统计");
        JavaRDD<String> map = rdd.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                String[] entry = v1.split(" ");
                acc.add(v1);
                return null;
            }
        });
        map.collect();
        System.out.println("value:" + acc.value());
    }
}
