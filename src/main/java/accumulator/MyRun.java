package accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class MyRun {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("testAccumulator");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        MyAccumulator acc = new MyAccumulator();

        sc.sc().register(acc,"PersonInfoAccumulator");
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "zhangsan 1", "lisi 2", "wangwu 3", "zhaoliu 4", "tianqi 5", "zhengba 6","zhaoliu 4"
        ));

        rdd.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                acc.add(new MyKey(1,Integer.parseInt(v1.split(" ")[1])));
                return v1;
            }
        }).collect();

        System.out.println("value = "+acc.value());

    }
}
