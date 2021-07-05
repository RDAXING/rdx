package accumulator;

import accumulator20200820.PersonAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class UserRun implements Serializable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UserRun").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
                "zhangsan 1", "lisi 2", "wangwu 3", "zhaoliu 4", "tianqi 5", "zhengba 6","zhaoliu 4"
        ),3);
        UserAccumulator acc = new UserAccumulator();
//        PersonAccumulator acc = new PersonAccumulator();
        sc.sc().register(acc,"userAcc");

        JavaRDD<String> map = rdd.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                acc.add(v1);
                return v1;
            }
        });
        map.collect();
        System.out.println(acc.value());

    }
}
