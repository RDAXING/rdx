package accumulator20200820;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class AccumulatorDemo implements Serializable {
    @Test
    public void AccumulatorString(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> dataRdd = sc.parallelize(Arrays.asList("a", "b", "c", "d"));
        AccumulatorString acc = new AccumulatorString();
        sc.sc().register(acc,"Accumulator");
        JavaRDD<String> filter = dataRdd.filter(f -> {
            if ("a".equals(f)) {
                return false;
            } else {
                acc.add(f);
                return true;
            }
        });
        filter.collect();
        System.out.println(acc.value());

    }

    @Test
    public void SparkMykey(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        MyKeyAccumulator acc = new MyKeyAccumulator();
        sc.sc().register(acc,"MyKeyAccumulator");
        JavaRDD<String> dataRdd = sc.parallelize(Arrays.asList("a 10", "heiil 23", "spark 26", "scala 76"));
        List<Tuple2<String, Integer>> collect = dataRdd.mapToPair(f -> {
            String[] value = f.split(" ");
            MyKey myKey = new MyKey(1, Integer.parseInt(value[1]));
            acc.add(myKey);
            return new Tuple2<>(f, 1);
        }).collect();
        System.out.println(acc.value());
    }


    @Test
    public void SparkPerson(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        PersonAccumulator acc = new PersonAccumulator();
        sc.sc().register(acc,"PersonAccumulator");
        JavaRDD<String> dataRdd = sc.parallelize(Arrays.asList("a 10", "heiil 23", "spark 26", "scala 76"));
        JavaRDD<String> map = dataRdd.map(new Function<String, String>() {
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
