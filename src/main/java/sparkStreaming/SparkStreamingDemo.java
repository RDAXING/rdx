package sparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.Test;
import scala.Array;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkStreamingDemo implements Serializable {
    /**
     * 通过nc 作为数据源进行Wordcount的数量统计
     */
    @Test
    public void sparkStreamingWordCount() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordcount");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 9999);

        JavaDStream<String> flatMapRdd = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                List<String> list = Arrays.asList(split);
                return list.iterator();
            }
        });

        JavaPairDStream<String, Integer> pairDStream = flatMapRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });


        JavaPairDStream<String, Integer> resdata = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        resdata.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
            @Override
            public void call(JavaPairRDD<String, Integer> v1, Time v2) throws Exception {
                System.err.println("count time:" + v2 + "," + v1.collect());
            }
        });


        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过nc 作为数据源进行Wordcount的数量统计
     * 并且设置检查点checkpoint
     * 通过updateStateByKey进行实时更新每批次统计的结果和
     */
    @Test
    public void SparkStreaming2(){
        Logger.getLogger("org").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("ss").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
        JavaReceiverInputDStream<String> dataDs = jsc.socketTextStream("localhost", 9999);
        jsc.checkpoint("C:\\Users\\admin\\Desktop\\sparktest\\checkpoint");
        JavaPairDStream<String, Integer> pairDStream = dataDs.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] split = s.split(" ");
                List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                for (String name : split) {
                    tuple2s.add(new Tuple2<>(name, 1));
                }
                return tuple2s.iterator();
            }
        });

        JavaPairDStream<String, Integer> resDS = pairDStream.reduceByKey((f1, f2) -> {
            return f1 + f2;
        });
        JavaPairDStream<String, Integer> pairDStream1 = resDS.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            @Override
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> stat) throws Exception {
                Integer optionstat = 0;
                if (stat.isPresent()) {
                    optionstat = stat.get();
                }

                for (Integer value : integers) {
                    optionstat += value;
                }
                return Optional.of(optionstat);
            }
        });
        pairDStream1.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
            @Override
            public void call(JavaPairRDD<String, Integer> v1, Time v2) throws Exception {
                System.err.println("当前时间为：" + v2 +",当前批次结果："+v1.collect());
            }
        });

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void SparkStreaming3(){
        Logger.getLogger("org").setLevel(Level.WARN);
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(chechpoint, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                return getStreaming();
            }
        });

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    String chechpoint = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\cccccc";
    public JavaStreamingContext getStreaming(){
        SparkConf conf = new SparkConf().setAppName("hhh").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
        jsc.checkpoint(chechpoint);
        JavaReceiverInputDStream<String> dataDs = jsc.socketTextStream("localhost", 9999);
        JavaPairDStream<String, Integer> pairDStream = dataDs.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] split = s.split(" ");
                List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                for (String name : split) {
                    tuple2s.add(new Tuple2<>(name, 1));
                }
                return tuple2s.iterator();
            }
        });

        JavaPairDStream<String, Integer> resDS = pairDStream.reduceByKey((f1, f2) -> {
            return f1 + f2;
        });
        JavaPairDStream<String, Integer> pairDStream1 = resDS.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            @Override
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> stat) throws Exception {
                Integer optionstat = 0;
                if (stat.isPresent()) {
                    optionstat = stat.get();
                }

                for (Integer value : integers) {
                    optionstat += value;
                }
                return Optional.of(optionstat);
            }
        });
        pairDStream1.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
            @Override
            public void call(JavaPairRDD<String, Integer> v1, Time v2) throws Exception {
                System.err.println("当前时间为：" + v2 +",当前批次结果："+v1.collect());
            }
        });
        return jsc;
    }

    /**
     * simple 使用window
     */
    @Test
    public void SparkStreamingWindow(){
        Logger.getLogger("org").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWindow");
        String checkppoint = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\hello";
        JavaStreamingContext jsc = new JavaStreamingContext(conf,Durations.seconds(1));
        jsc.checkpoint(checkppoint);
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 8989);
        JavaDStream<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaDStream<String> window = word.window(Durations.seconds(3), Durations.seconds(2));
        window.print();
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void SparkStreamingWindow1(){
        Logger.getLogger("org").setLevel(Level.WARN);
        String checkpoint = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\hello";
        SparkConf conf = new SparkConf().setAppName("SparkStreamingWindow1").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
        jsc.checkpoint(checkpoint);
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 8989);
        JavaPairDStream<String, Integer> pairDStream = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] split = s.split(" ");
                List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                for (String name : split) {
                    list.add(new Tuple2<>(name, 1));
                }
                return list.iterator();
            }
        });

        JavaPairDStream<String, Integer> windowDS = pairDStream.reduceByKey((f1, f2) -> {
            return f1 + f2;
        }).window(Durations.seconds(8), Durations.seconds(8));

        JavaPairDStream<String, Integer> updateStateByKey = windowDS.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            @Override
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> optional) throws Exception {
                Integer optionalValue = 0;
                if (optional.isPresent()) {
                    optionalValue = optional.get();
                }

                for (Integer num : integers) {
                    optionalValue += num;
                }
                return Optional.of(optionalValue);
            }
        });

        updateStateByKey.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
            @Override
            public void call(JavaPairRDD<String, Integer> pairRdd, Time time) throws Exception {
                System.out.println("当前时间：" + time + ",批次结果：" +pairRdd.collect());
            }
        });

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * countByWindow的使用
     */
    @Test
    public void SparkStringCountByWindow(){
        Logger.getLogger("org").setLevel(Level.WARN);
        String checkpoint = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\hello";
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 8989);
        jsc.checkpoint(checkpoint);
        JavaDStream<String> flatMapDS = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = Arrays.asList(s.split(" "));
                return list.iterator();
            }
        });

        JavaDStream<Long> resDS = flatMapDS.countByWindow(Durations.seconds(6), Durations.seconds(6));


        resDS.print();;
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void SparkStreamingReduceByWindow(){
        Logger.getLogger("org").setLevel(Level.WARN);
        String checkpoint = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\hello";
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingReduceByWindow");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        jsc.checkpoint(checkpoint);
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 8989);
        JavaPairDStream<String, Integer> pairDStream = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                for (String name : s.split(" ")) {
                    tuple2s.add(new Tuple2<>(name, 1));
                }
                return tuple2s.iterator();
            }
        });

        JavaPairDStream<String, Integer> dStream = pairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, Durations.seconds(3), Durations.seconds(1));

        dStream.print();
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * start
     */
    private static String chechpath = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\hello";
    @Test
    public void SparkStreamingReducebyWindow(){
        Logger.getLogger("org").setLevel(Level.WARN);
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(chechpath, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                return getStreamingContext();
            }
        });
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static  JavaStreamingContext getStreamingContext(){
        Logger.getLogger("org").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingReduceByWindow");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        jsc.checkpoint(chechpath);
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 8989);
        JavaPairDStream<String, Integer> pairDStream = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                for (String name : s.split(" ")) {
                    tuple2s.add(new Tuple2<>(name, 1));
                }
                return tuple2s.iterator();
            }
        });

        JavaPairDStream<String, Integer> dStream = pairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, Durations.seconds(3), Durations.seconds(3));

        dStream.print();
        return  jsc;
    }

    /**
     * end
     */

    /**
     * start
     */
    private static String checkpointPath = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\a2";
    @Test
    public void SparkStreamingWorldCount(){
        Logger.getLogger("org").setLevel(Level.WARN);
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointPath, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                return getWorldCount();
            }
        });
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static JavaStreamingContext getWorldCount(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
        jsc.checkpoint(checkpointPath);
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
        JavaPairDStream<String, Integer> pairDStream = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                for (String name : s.split(" ")) {
                    tuple2s.add(new Tuple2<>(name, 1));
                }
                return tuple2s.iterator();
            }
        }).reduceByKey((f1, f2) -> {
            return f1 + f2;
        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            @Override
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {

                Integer optionalValue = 0;
                if (v2.isPresent()) {
                    optionalValue = v2.get();
                }

                for (Integer i : v1) {
                    optionalValue += i;
                }
                return Optional.of(optionalValue);
            }
        });


        return  jsc;
    }


    /**
     * end
     */

    @Test
    public void SparkStreamingReadFile(){
        String checkpath = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\hello";
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpath, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
//                Logger.getLogger("org").setLevel(Level.WARN);
                SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingReadFile");
                JavaSparkContext sc = new JavaSparkContext(conf);
                JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
                jsc.checkpoint(checkpath);
                JavaDStream<String> dataDS = jsc.textFileStream("C:\\Users\\admin\\Desktop\\sparktest\\hadoopdir");
                JavaPairDStream<String, Integer> pairDStream = dataDS.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                        String[] split = s.split(" ");
                        List<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                        for (String name : split) {
                            tuple2s.add(new Tuple2<>(name, 1));
                        }
                        return tuple2s.iterator();
                    }
                }).reduceByKey((f1, f2) -> {
                    return f1 + f2;
                });
                pairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                    @Override
                    public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                        Integer sum = 0;
                        if(v2.isPresent()){
                            sum = v2.get();
                        }
                        for(Integer i : v1){
                            sum  += i;
                        }
                        return Optional.of(sum);
                    }
                }).print();
                return jsc;
            }
        });


        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
