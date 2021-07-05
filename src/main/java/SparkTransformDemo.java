import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class SparkTransformDemo implements Serializable{
    @Test
    public void SparkDemoMap(){
        SparkConf conf = new SparkConf().setAppName("SparkDemoMap").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> listdata = new ArrayList<>();
        listdata.add("1,2,3,4,5,6");
        listdata.add("a,b,c,d,e,f");
        JavaRDD<String> rdd1 = sc.parallelize(listdata);
        JavaRDD<String[]> maprdd = rdd1.map(
                new Function<String, String[]>() {
                    @Override
                    public String[] call(String v1) throws Exception {
                        return v1.split(",");
                    }
                }
        );
        maprdd.foreach(f ->{
            for(int i=0;i<f.length;i++){
                System.out.print(f[i]);
                System.out.println();
            }
        });
    }


    @Test
    public void SparkFlatMapDemo(){
        SparkConf conf = new SparkConf().setAppName("SparkFlatMapDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> listdata = new ArrayList<>();
        listdata.add("1,2,3,4,5,6");
        listdata.add("a,b,c,d,e,f");
        JavaRDD<String> datardd = sc.parallelize(listdata);
        JavaRDD<String> resrdd = datardd.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return new ArrayList<String>(Arrays.asList(s.split(","))).iterator();
                    }
                }
        );

        long count = resrdd.count();
        System.out.println(count);
    }

    @Test
    public void SparkFilterDemo(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkFilterDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> number = new ArrayList<>();
        for(int i=1;i<=100;i++){
            number.add(i);
        }

        JavaRDD<Integer> dataRdd = sc.parallelize(number);
        JavaRDD<Integer> filter = dataRdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                if (v1 % 2 == 0) {
                    return true;
                } else {

                    return false;
                }
            }
        });

        List<Integer> collect = filter.collect();
        System.out.println(collect);
    }

    @Test
    public void SparkUnionDemo(){
        SparkConf conf = new SparkConf().setAppName("SparkUnionDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String s1 = "a,b,c,a,e,c,a";
        String s2 = "1,2,3,4,5,6,7";
        List<String> data1 = Arrays.asList(s1.split(","));
        List<String> data2 = Arrays.asList(s2.split(","));
        JavaRDD<String> rdd1 = sc.parallelize(data1);
        JavaRDD<String> rdd2 = sc.parallelize(data2);

        JavaPairRDD<String, String> tupleRdd = rdd1.zip(rdd2);
//        List<Tuple2<String, String>> tuple = zip.collect();
//        System.out.println(tuple);

        JavaPairRDD<String, Iterable<String>> res = tupleRdd.groupByKey();
        List<Tuple2<String, Iterable<String>>> collect = res.collect();
        System.out.println(collect);
    }

    @Test
    public void SparkWordCountDemo(){
        SparkConf conf = new SparkConf().setAppName("SparkWordCountDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "C:\\Users\\admin\\Desktop\\sparktest\\a.txt";
        JavaRDD<String> dataRdd = sc.textFile(path);
        JavaRDD<String> flatMapRdd = dataRdd.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        String[] split = s.split(" ");
                        List<String> list = Arrays.asList(split);
                        Iterator<String> iterator = list.iterator();
                        return iterator;
                    }
                }
        );

        JavaPairRDD<String, Integer> mapRdd = flatMapRdd.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {

                        return new Tuple2<>(s, 1);
                    }
                }
        );

        JavaPairRDD<String, Integer> reduceByKeyRdd = mapRdd.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        List<Tuple2<String, Integer>> collect = reduceByKeyRdd.collect();
        System.out.println(collect);
        reduceByKeyRdd.saveAsTextFile("C:\\Users\\admin\\Desktop\\sparktest\\b.txt");

    }
}
