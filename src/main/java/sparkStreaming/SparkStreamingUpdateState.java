package sparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 将nc作为数据源，通过Sparkstreaming对数据进行实时计算
 * getOrCreate(checkpoint, new Function0<JavaStreamingContext>()如果程序中断，或者停止运行时，然后从新启动从新，
 * 保证数据不会丢失，会接着从上次的结果再次统计结果，并且数据源一直在灌入数据也不影响统计结果的丢失
 */
public class SparkStreamingUpdateState implements Serializable {
    private static String checkpoint = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint\\cccccc";
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpoint, new Function0<JavaStreamingContext>() {
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

    public static  JavaStreamingContext getStreaming(){
        SparkConf conf = new SparkConf().setAppName("SparkStreamingUpdateState").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
        jsc.checkpoint(checkpoint);
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
}
