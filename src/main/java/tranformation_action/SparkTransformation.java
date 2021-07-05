package tranformation_action;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

public class SparkTransformation implements Serializable{
    private static SparkConf conf;
    private static JavaSparkContext sc;
    static {
        conf = new SparkConf().setAppName("SparkTransformation")
                .setMaster("local[*]");
        sc = new JavaSparkContext(conf);
    }

    @Test
    public void SparkRDD_map(){
        JavaRDD<String> dataRdd = sc.parallelize(Arrays.asList("hello,java,spark,scala", "hbase,redis,hadoop,hive,elasticsearch"));
        JavaRDD<List<String>> map = dataRdd.map(new Function<String, List<String>>() {
            @Override
            public List<String> call(String s) throws Exception {
                String[] split = s.split(",");
                List<String> list = Arrays.asList(split);
                return list;
            }
        });
        System.err.println(map.collect());
        JavaRDD<String> stringJavaRDD = map.flatMap(new FlatMapFunction<List<String>, String>() {
            @Override
            public Iterator<String> call(List<String> strings) throws Exception {
                List<String> list = new ArrayList<>();
                list.addAll(strings);
                return list.iterator();
            }
        });
        List<String> collect = stringJavaRDD.collect();
        System.err.println(collect);

        JavaRDD<String> filterRdd = stringJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.contains("a") || s.contains("s")) {
                    return false;
                } else
                    return true;
            }
        });
        System.err.println(filterRdd.collect());
    }

    @Test
    public void SparkUnion(){
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 21, 3, 4, 5,6,7));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(6, 7, 8, 21, 15,23));
        JavaRDD<Integer> resRdd = rdd1.union(rdd2);
        System.err.println(resRdd.collect());
        List<Integer> collect = rdd1.intersection(rdd2).collect();

        System.err.println(collect);



    }

    @Test
    public void SparkPairRdd(){
        JavaRDD<String> dataRdd = sc.parallelize(Arrays.asList("a 1", "a 32","b 2","b 77","c 27", "c 34", "f 5"));
        JavaPairRDD<String, String> pairRdd = dataRdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2(split[0], split[1]);
            }
        });
        System.err.println(pairRdd.groupByKey().collect());

        JavaPairRDD<String, String> mapvaluesRdd = pairRdd.mapValues(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                return s + "_hello";
            }
        });
        System.err.println(mapvaluesRdd.collect());
    }

    @Test
    public void SparkJOin(){
        List<Tuple2<String, Integer>> tuple1 = new ArrayList<>();
        tuple1.add(new Tuple2<>("a",1));
        tuple1.add(new Tuple2<>("b",32));
        tuple1.add(new Tuple2<>("a",16));
        tuple1.add(new Tuple2<>("r",8));
        tuple1.add(new Tuple2<>("a",10));
        tuple1.add(new Tuple2<>("w",12));
        tuple1.add(new Tuple2<>("q",13));
        tuple1.add(new Tuple2<>("j",6));

        List<Tuple2<String, Integer>> tuple2 = new ArrayList<>();
        tuple2.add(new Tuple2<>("a",1));
        tuple2.add(new Tuple2<>("b",32));
        tuple2.add(new Tuple2<>("c",16));
        tuple2.add(new Tuple2<>("r",8));
        tuple2.add(new Tuple2<>("a",10));
        tuple2.add(new Tuple2<>("w",12));
        tuple2.add(new Tuple2<>("q",13));
//        tuple2.add(new Tuple2<>("j",6));
        JavaPairRDD<String, Integer> pairRDD1 = sc.parallelizePairs(tuple1);
        JavaPairRDD<String, Integer> pairRDD2 = sc.parallelizePairs(tuple2);
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRdd = pairRDD1.join(pairRDD2);
        System.err.println(joinRdd.collect());

        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupRdd = pairRDD1.cogroup(pairRDD2);
        System.err.println(cogroupRdd.collect());

    }

    @Test
    public void SparkMapParatitions(){
        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 12, 13, 14, 15), 3);
        JavaRDD<Map<String, Integer>> mapJavaRDD = dataRdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Map<String, Integer>>() {
            @Override
            public Iterator<Map<String, Integer>> call(Iterator<Integer> integerIterator) throws Exception {
                int sum = 0;
                while (integerIterator.hasNext()) {
                    Integer num = integerIterator.next();
                    sum += num;
                }
                int partitionId = TaskContext.getPartitionId();
                HashMap<String, Integer> map = new HashMap<>();
                map.put("当前分区：" + partitionId, sum);
                List<Map<String, Integer>> maps = new ArrayList<>();
                maps.add(map);
                return maps.iterator();
            }
        });
        List<Map<String, Integer>> collect = mapJavaRDD.collect();
        System.err.println(collect);
    }

    @Test
    public void SparkMapPartitionsWithx(){
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
//RDD有两个分区
        JavaRDD<Integer> javaRDD = sc.parallelize(data,2);
//分区index、元素值、元素编号输出
        JavaRDD<String> resRdd = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
                LinkedList<String> linkedList = new LinkedList<String>();
                int i = 0;
                while (v2.hasNext())
                    linkedList.add(Integer.toString(v1) + "|" + v2.next().toString() + Integer.toString(i++));
                return linkedList.iterator();
            }
        },false);
        System.err.println(resRdd.collect());
    }

    @Test
    public void SparkCoalCase(){
        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(12, 30), 3);
        JavaRDD<Integer> repattitionRdd = dataRdd.coalesce(2);
        JavaRDD<Integer> shuffleRdd = dataRdd.coalesce(10, true);
        System.err.println("元分区为："+dataRdd.getNumPartitions() + "减少分区为：" +repattitionRdd.getNumPartitions()+"增加的分区：" + shuffleRdd.getNumPartitions());
    }


    @Test
    public void SparDistinct(){
        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(1, 3, 5, 7, 9, 8, 5, 3, 1, 5, 7, 5, 3));
        JavaRDD<Integer> distinct = dataRdd.distinct();
        System.err.println(distinct.collect());
    }

    @Test
    public void SparkJoinLeftRight(){
        List<Tuple2<String, Integer>> tuple1 = new ArrayList<>();
        tuple1.add(new Tuple2<>("a",1));
        tuple1.add(new Tuple2<>("b",32));
        tuple1.add(new Tuple2<>("c",16));
        tuple1.add(new Tuple2<>("r",8));
        tuple1.add(new Tuple2<>("f",54));
        tuple1.add(new Tuple2<>("w",12));
        tuple1.add(new Tuple2<>("q",13));
        tuple1.add(new Tuple2<>("j",6));

        List<Tuple2<String, Integer>> tuple2 = new ArrayList<>();
        tuple2.add(new Tuple2<>("a",1));
        tuple2.add(new Tuple2<>("p",32));
        tuple2.add(new Tuple2<>("c",16));
        tuple2.add(new Tuple2<>("o",8));
        tuple2.add(new Tuple2<>("f",10));
        tuple2.add(new Tuple2<>("s",12));
        tuple2.add(new Tuple2<>("n",13));
        tuple2.add(new Tuple2<>("m",6));
        JavaPairRDD<String, Integer> pairRDD1 = sc.parallelizePairs(tuple1);
        JavaPairRDD<String, Integer> pairRDD2 = sc.parallelizePairs(tuple2);
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> res1 = pairRDD1.leftOuterJoin(pairRDD2);
        JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> res2 = pairRDD1.rightOuterJoin(pairRDD2);
        System.out.println(res1.collect());
        System.err.println(res2.collect());
    }
}
