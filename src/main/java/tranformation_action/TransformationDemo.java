package tranformation_action;

import accumulator1.MyAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class TransformationDemo implements Serializable {
    private static SparkConf conf;
    private static JavaSparkContext sc;
    static {
        conf = new SparkConf().setAppName("TransformationDemo").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
    }

    /**
     * worldcount：
     * flatMap,mapToPair,reduceByKey,sortBykeyRdd 转换算子的使用
     * collect 行动算子的使用
     */
    @Test
    public void SparkWorldCount(){
        String inpath = "C:\\Users\\admin\\Desktop\\sparktest\\a.txt";
        JavaRDD<String> dataRdd = sc.textFile(inpath);
        JavaRDD<String> flatMapRdd = dataRdd.flatMap(f -> {
            String[] value = f.split(" ");
            List<String> list = Arrays.asList(value);
            return list.iterator();
        });

        System.out.println(flatMapRdd.collect());
        JavaPairRDD<String, Integer> pairRDD = flatMapRdd.mapToPair(f -> {
            return new Tuple2<>(f, 1);
        });

        System.out.println(pairRDD.collect());

        JavaPairRDD<String, Integer> reduceRdd = pairRDD.reduceByKey((f1, f2) -> {
            return f1 + f2
                    ;
        });

        List<Tuple2<String, Integer>> collect = reduceRdd.collect();
        System.out.println(collect);

        JavaPairRDD<String, Integer> sortBykeyRdd = reduceRdd.sortByKey();
        System.out.println(sortBykeyRdd.collect());
    }

    /**
     * 检查点checkpoint，广播变量broadcast,累加器accumulator
     */
    @Test
    public void SparkBroadCastAccCheck(){
        List<String> list = Arrays.asList("a,c,e,f,h,i,ha,sz,m".split(","));
        String checkpointdir = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint";
        String inpath = "C:\\Users\\admin\\Desktop\\sparktest\\a.txt";
        //设置检查点
        sc.setCheckpointDir(checkpointdir);

        Broadcast<List<String>> broadcast = sc.broadcast(list);
        JavaRDD<String> dataRdd = sc.textFile(inpath);
        JavaRDD<String> filterRdd = dataRdd.flatMap(f -> {
            List<String> lista = new ArrayList<>();
            String[] split = f.split(" ");
            lista = Arrays.asList(split);
            return lista.iterator();
        }).filter(f -> {
            List<String> value = broadcast.getValue();
            if (value.contains(f)) {
                return true;
            } else {
                return false;
            }
        });
        filterRdd.checkpoint();
        System.out.println(filterRdd.collect());

    }
    @Test
    public void SparkMyAccumulator(){
        MyAccumulator myAccumulator = new MyAccumulator();
        HashSet<String> blacklist = new HashSet<>();
        blacklist.add("jack");
        sc.sc().register(myAccumulator,"myAccumulator");
        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaRDD<String> filter = dataRDD.filter(f -> {
            if (blacklist.contains(f)) {
                return true;
            } else {
                myAccumulator.add(f);
                return false;
            }
        });
        filter.collect();
        System.out.println(myAccumulator.value());
    }
}
