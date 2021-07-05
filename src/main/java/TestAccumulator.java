import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.HashSet;

/**
 * spark内置了数值类型的累加器，比如LongAccumulator、DoubleAccumulator
 */
public class TestAccumulator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        SparkSession spark = SparkSession
                .builder()
                .appName("gxl")
                .master("local")
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        testMyAccumulator(jsc);

    }

    private static void testMyAccumulator(JavaSparkContext jsc){
        MyAccumulator myAccumulator = new MyAccumulator();
        jsc.sc().register(myAccumulator,"myAccumulator");

        HashSet<String> blacklist = new HashSet<>();
        blacklist.add("jack");

        JavaRDD<String> stringJavaRDD = jsc.parallelize(Arrays.asList("jack", "kevin", "wade", "james"));
        JavaRDD<String> filter = stringJavaRDD.filter((Function<String, Boolean>) v1 -> {
            if (blacklist.contains(v1)) {
                return true;
            } else {
                myAccumulator.add(v1);
                return false;
            }
        });
        filter.count();
        System.out.println(myAccumulator.value());
    }

}