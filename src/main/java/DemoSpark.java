import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class DemoSpark implements Serializable {
    @Test
    public void SparkMap(){
        SparkConf conf = new SparkConf().setAppName("SparkMap").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("a,b,c,d,rf,tg", "hello,world,spark,java,scala,python,redis,hive,hbase");
        JavaRDD<String> dataRdd = sc.parallelize(list);
        JavaRDD<Map> map = dataRdd.map(
                new Function<String, Map>() {
                    @Override
                    public Map call(String v1) throws Exception {
                        String[] split = v1.split(",");
                        Map<String, Integer> maps = new HashMap<>();
                        for (String ss : split) {
                            maps.put(ss, 1);
                        }
                        return maps;
                    }
                }
        );
        System.out.println(map.collect().size());
        //flatmap
        JavaRDD<Map> mapJavaRDD = map.flatMap(
                new FlatMapFunction<Map, Map>() {
                    @Override
                    public Iterator<Map> call(Map map) throws Exception {
                        List<Map> maps = new ArrayList<>();
                        maps.add(map);

                        return maps.iterator();
                    }
                }
        );
        System.out.println(mapJavaRDD.collect().size());


    }

    @Test
    public void SparkFlatMap(){
        SparkConf conf = new SparkConf().setAppName("SparkMap").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("a,b,c,d,rf,tg,r,a,df", "hello,world,spark,java,scala,python,redis,hive,hbase");
        JavaRDD<String> dataRdd = sc.parallelize(list);
        List<String> res = dataRdd.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        String[] ss = s.split(",");
                        List list = Arrays.asList(ss);

                        return list.iterator();
                    }
                }
        ).collect();
        System.out.println(res);

        JavaRDD<String> stringJavaRDD = dataRdd.flatMap(f -> {
            String[] ss = f.split(",");
            List<String> list1 = Arrays.asList(ss);
            return list1.iterator();
        });


        JavaPairRDD<String, Integer> resdata = stringJavaRDD.mapToPair(f -> {
            return new Tuple2<>(f, 1);
        });
        JavaPairRDD<String, Integer> resss = resdata.reduceByKey((f1, f2) -> {
            return f1 + f2;
        });
        System.out.println(resss.collect());

    }

    @Test
    public void SparkFilter(){
        SparkConf conf = new SparkConf().setAppName("SparkFilterDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String ss = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";
        List<String> num = new ArrayList<>(Arrays.asList(ss.split(",")));
        JavaRDD<String> dataRdd = sc.parallelize(num);
        JavaRDD<String> res = dataRdd.filter(f -> {
            int count = Integer.parseInt(f);
            return count % 2 == 0;
        });

        System.out.println(res.collect());

    }


    @Test
    public void SparkGroupByKey(){
        SparkConf conf = new SparkConf().setAppName("SparkFCon").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> datalist = Arrays.asList(new Tuple2<>("spark", "day1"),
                new Tuple2<>("spark", "day2"),
                new Tuple2<>("hive", "day1"),
                new Tuple2<>("hive", "day2"),
                new Tuple2<>("scala", "day3"),
                new Tuple2<>("hive", "day3"));
        JavaPairRDD<String, String> dataRdd = sc.parallelizePairs(datalist);
        JavaPairRDD<String, Iterable<String>> res = dataRdd.groupByKey();
        System.out.println(res.collect());

    }


    @Test
    public void SparkMapValues(){
        SparkConf conf = new SparkConf().setAppName("SparkFCon").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> datalist = Arrays.asList(new Tuple2<>("spark", "day1"),
                new Tuple2<>("spark", "day2"),
                new Tuple2<>("hive", "day1"),
                new Tuple2<>("hive", "day2"),
                new Tuple2<>("scala", "day3"),
                new Tuple2<>("hive", "day3"));
        JavaPairRDD<String, String> dataRdd = sc.parallelizePairs(datalist);

        JavaPairRDD<String, String> resdata = dataRdd.mapValues(f -> {

            if(f.contains("2")){
                f = f + "hahah";
            }
            return f;
        });
        System.out.println(resdata.collect());
    }

    @Test
    public void SparkJoinDemo(){
        SparkConf conf = new SparkConf().setAppName("SparkJoinDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
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
        JavaPairRDD<String, Integer> one = sc.parallelizePairs(tuple1);
        JavaPairRDD<String, Integer> two = sc.parallelizePairs(tuple2);
        JavaPairRDD<String, Tuple2<Integer, Integer>> join = one.join(two);
        System.out.println(join.collect());
    }

    @Test
    public void SparkMapPartitions(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String file = "C:\\Users\\admin\\Desktop\\sparktest\\companyname";
        JavaRDD<String> dataRdd = sc.textFile(file,3);
//        dataRdd.foreach( f ->{
//            System.out.println(f);
//        });
        JavaRDD<String> stringJavaRDD = dataRdd.mapPartitions(f -> {
            List<String> reslist = new ArrayList<>();
            while (f.hasNext()) {
                String res = f.next();
                if (res.contains("四川") || res.contains("重庆")) {
                    reslist.add(res);
                }
            }
            int partitionId = TaskContext.getPartitionId();
            System.out.println("当前分区为：" + partitionId + ";其值为：" + StringUtils.join(reslist,","));

            return reslist.iterator();
        });

        stringJavaRDD.saveAsTextFile("C:\\Users\\admin\\Desktop\\sparktest\\b");
    }

    /**
     * Transformation(8)—fullOuterJoin、leftOuterJoin、rightOuterJoin
     */
    @Test
    public void SprkJoinlief_rigth_full(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);//Sparkc
        List<String> ss = Arrays.asList("a,b,v,c,d,e,t,f".split(","));
        JavaRDD<String> dataRdd = sc.parallelize(ss);
        JavaPairRDD<String, Integer> resRdd = dataRdd.mapToPair(f -> {
            Random random = new Random();
            int i = random.nextInt(10);
            return new Tuple2<>(f, i);
        });

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> r = resRdd.fullOuterJoin(resRdd,2);
        System.out.println(r.collect());

    }
}
