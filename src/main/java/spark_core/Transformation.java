package spark_core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Transformation implements Serializable{
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    @Test
    public void SparkMap(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkmap");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> datalist = new ArrayList<>();
        datalist.add("1,2,3,4,5,6,7,8,9");
        datalist.add("a,b,c,d,e,f,g,h,i");
        JavaRDD<String> dataRdd = sc.parallelize(datalist);
        JavaRDD<String[]> map = dataRdd.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                String[] split = s.split(",");

                return split;
            }
        });
        map.foreach(new VoidFunction<String[]>() {
            @Override
            public void call(String[] strings) throws Exception {
                for(String name : strings){
                    System.out.println(name);
                }
            }
        });
    }

    @Test
    public void SparkFlatMap(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        List<String> datalist = new ArrayList<>();
        datalist.add("1,2,3,4,5,6,7,8,9");
        datalist.add("a,b,c,d,e,f,g,h,i");
        JavaRDD<String> dataRdd = jsc.parallelize(datalist);
        JavaRDD<String> flatMapRdd = dataRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(",");
                List<String> list = Arrays.asList(split);
                return list.iterator();
            }
        });

        System.out.println(flatMapRdd.collect());
    }

    @Test
    public void SparkFilter(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> datalist = new ArrayList<>();
        datalist.add("1,2,3,4,5,6,7,8,9");
        datalist.add("a,b,c,d,好,f,g,h,i");
        JavaRDD<String> dataRdd = sc.parallelize(datalist);
        JavaRDD<String> filter = dataRdd.flatMap(f -> {
            List<String> list = Arrays.asList(f.split(","));
            return list.iterator();
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String rex = "[\u4e00-\u9fa5]";
//                Pattern pattern = Pattern.compile("[\u4e00-\u9fa5]");
//                Matcher matcher = pattern.matcher(s);
                if (s.matches(rex))
                    return true;
                else
                    return false;


            }
        });
        System.out.println(filter.collect());
    }

    @Test
    public void SparkUnion(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        String a = "1,2,3,4,5,6,7,8,9";
        String b = "a,b,c,d,e,f,g,h,i";
        JavaRDD<String> aRdd = jsc.parallelize(Arrays.asList(a.split(",")));
        JavaRDD<String> bRdd = jsc.parallelize(Arrays.asList(b.split(",")));
        JavaRDD<String> union = aRdd.union(bRdd);
        System.out.println(union.collect());
        JavaPairRDD<String, String> zip = aRdd.zip(bRdd);
        System.out.println(zip.collect());
    }


    @Test
    public void SparkgroupBykey(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        SparkContext sc = new SparkContext(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        String name = "a,v,d,s,a,z,c,f,d,gv,s,s,a,grg,zf,v,z";
        JavaRDD<String> dataRdd = jsc.parallelize(Arrays.asList(name.split(",")));
        JavaPairRDD<String, Integer> resRdd = dataRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Iterator<String> value) throws Exception {
                List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                while (value.hasNext()) {
                    String next = value.next();
                    int i = new Random().nextInt(100);
                    tuple2s.add(new Tuple2<>(next, i));
                }


                return tuple2s.iterator();
            }
        });
        JavaPairRDD<String, Integer> cache = resRdd.cache();
        JavaPairRDD<String, Iterable<Integer>> groupByKey = cache.groupByKey();
        System.out.println(groupByKey.collect());
        JavaPairRDD<String, Integer> pairRDD = groupByKey.mapValues(new Function<Iterable<Integer>, Integer>() {
            @Override
            public Integer call(Iterable<Integer> integers) throws Exception {
                Integer sum = 0;
                for (Integer i : integers) {
                    sum += i;
                }
                return sum;
            }
        });

        System.out.println(pairRDD.collect());

        JavaPairRDD<String, Integer> pairRDD1 = pairRDD.sortByKey(false);
        System.out.println(pairRDD1.collect());
    }

    /**
     * join/cogroup/groupByKey/union/zip/map/mapvalues/mappartations/flatmap/filter
     *已经处理，剩余的 明天处理
     */
}
