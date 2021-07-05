import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class SparkWordCount implements Serializable {
    static{
        Logger.getLogger("org").setLevel(Level.WARN);
    }
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SaprkWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\a.txt");
        JavaRDD<String> restdd = fileRdd.flatMap(f -> {
            String[] split = f.split(" ");
            List<String> lists = Arrays.asList(split);
            return lists.iterator();
        });
        restdd.foreach( f ->{
            System.out.println(f);
        });
    }


    /**
     * spark ----map
     */
   @Test
    public void SparkJonDemo(){
       SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       List<String> datalist = new ArrayList<>();
        datalist.add("1,2,3,4,5,6,7,8,9");
        datalist.add("a,b,c,d,e,f,g,h,i");
       JavaRDD<String> datardd = sc.parallelize(datalist);
       JavaRDD<String[]> maprdd = datardd.map(
               new Function<String, String[]>() {
                   @Override
                   public String[] call(String v1) throws Exception {
                       return v1.split(",");
                   }
               }
       );
       JavaRDD<String[]> cacheRdd = maprdd.cache();
       List<String[]> collect = cacheRdd.collect();
       for(String[] ss:collect){
           List<String> strings = Arrays.asList(ss);
           System.out.println(strings);
       }
   }

    /**
     * spark -----flatmap
     */
   @Test
    public void SparkFlatMapDemocracy(){
       SparkConf conf = new SparkConf().setAppName("SparkFlatMapDemocracy").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       List<String> datalist = new ArrayList<>();
       datalist.add("1,2,3,4,5,6,7,8,9");
       datalist.add("a,b,c,d,e,f,g,h,i");
       JavaRDD<String> dataRdd = sc.parallelize(datalist);
       JavaRDD<String> resRdd = dataRdd.flatMap(
               new FlatMapFunction<String, String>() {
                   @Override
                   public Iterator<String> call(String s) throws Exception {
                       String[] split = s.split(",");
                       List<String> list = Arrays.asList(split);

                       return list.iterator();
                   }
               }
       );


       JavaRDD<String> cache = resRdd.cache();
       List<String> res = cache.collect();
       System.out.println(res);
       FileUtils.deleteQuietly(new File("C:\\Users\\admin\\Desktop\\sparktest\\b.data"));
       resRdd.saveAsTextFile("C:\\Users\\admin\\Desktop\\sparktest\\b.data");
   }

    /**
     * spark -------filter
     */
   @Test
    public void SparkFilterDemo(){
       SparkConf conf = new SparkConf().setAppName("SparkFilterDemo").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       String ss = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";
       List<String> num = new ArrayList<>(Arrays.asList(ss.split(",")));
       JavaRDD<String> dataRdd = sc.parallelize(num);
       JavaRDD<String> filter = dataRdd.filter(
               new Function<String, Boolean>() {
                   @Override
                   public Boolean call(String v1) throws Exception {
                       int num = Integer.parseInt(v1);
                       if (num % 2 == 0)
                           return true;
                       return false;
                   }
               }
       );

       JavaRDD<String> cache = filter.cache();
       List<String> collect = cache.collect();
       System.out.println(collect);

   }

    /**
     * spark ---------union
     */
   @Test
    public void SparkUnionDemo(){
       SparkConf conf = new SparkConf().setAppName("SparkUnionDemo").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       String a = "1,2,3,4,5";
       String b = "a,b,c,d,r";
       List<String> list1 = new ArrayList<>(Arrays.asList(a.split(",")));
       List<String> list2 = new ArrayList<>(Arrays.asList(b.split(",")));
       JavaRDD<String> rdd1 = sc.parallelize(list1);
       JavaRDD<String> rdd2 = sc.parallelize(list2);
       JavaRDD<String> union = rdd1.union(rdd2);
       JavaRDD<String> cache = union.cache();
       List<String> collect = cache.collect();
       System.out.println(collect);
   }


    /**
     * spark --------GroupByKey
     */
   @Test
    public void SparkGroupByKeyDemo(){
       SparkConf conf = new SparkConf().setAppName("SparkGroupByKeyDemo").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       List list=Arrays.asList(new Tuple2<String,String>("c1","cai"),new Tuple2<String,String>("c2","niao"),
               new Tuple2<String,String>("c1","huo"),new Tuple2<String,String>("c2","niao"));
       JavaPairRDD dataRdd = sc.parallelizePairs(list);
       JavaPairRDD<String, Iterable<String>> groupRdd=dataRdd.groupByKey();
       System.out.println(groupRdd.collect());
       groupRdd.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>(){

           @Override
           public void call(Tuple2<String, Iterable<String>> tuple)
                   throws Exception {
               // TODO Auto-generated method stub

               System.out.println("key:"+tuple._1);
               Iterator<String> it=tuple._2.iterator();
               while(it.hasNext())
                   System.out.println("-----values:"+it.next());
           }

       });
   }


    /**
     * spark --------mapvalues的使用
     */
   @Test
    public void SparkMapValuesDemo(){
       SparkConf conf = new SparkConf().setAppName("SparkMapValuesDemo").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       List<Tuple2<String, Integer>> tuple = new ArrayList<>();
       tuple.add(new Tuple2<>("a",1));
       tuple.add(new Tuple2<>("b",32));
       tuple.add(new Tuple2<>("c",16));
       tuple.add(new Tuple2<>("r",8));
       tuple.add(new Tuple2<>("f",10));
       tuple.add(new Tuple2<>("w",12));
       tuple.add(new Tuple2<>("q",13));
       tuple.add(new Tuple2<>("j",6));

       JavaPairRDD<String, Integer> dataRdd = sc.parallelizePairs(tuple);
       JavaPairRDD<String, String> res = dataRdd.mapValues(
               new Function<Integer, String>() {
                   @Override
                   public String call(Integer v1) throws Exception {
                       return v1 + "hello";
                   }
               }
       );
       List<Tuple2<String, String>> collect = res.collect();
       System.out.println(collect);
   }


    /**
     * Sparek ------ join
     * [(a,(1,1)), (q,(13,22)), (b,(32,32)), (j,(6,6)), (r,(8,8)), (f,(10,10)), (w,(12,12))]
     */
   @Test
    public void SparkJoinDemo(){
       SparkConf conf = new SparkConf().setAppName("SparkJoinDemo").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       List<Tuple2<String, Integer>> tuple1 = new ArrayList<>();
       tuple1.add(new Tuple2<>("a",1));
       tuple1.add(new Tuple2<>("b",32));
       tuple1.add(new Tuple2<>("m",16));
       tuple1.add(new Tuple2<>("r",8));
       tuple1.add(new Tuple2<>("f",10));
       tuple1.add(new Tuple2<>("w",12));
       tuple1.add(new Tuple2<>("q",13));
       tuple1.add(new Tuple2<>("j",6));

       List<Tuple2<String, Integer>> tuple2 = new ArrayList<>();
       tuple2.add(new Tuple2<>("a",1));
       tuple2.add(new Tuple2<>("b",32));
       tuple2.add(new Tuple2<>("c",16));
       tuple2.add(new Tuple2<>("r",8));
       tuple2.add(new Tuple2<>("f",10));
       tuple2.add(new Tuple2<>("w",12));
       tuple2.add(new Tuple2<>("q",22));
       tuple2.add(new Tuple2<>("j",6));

       JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(tuple1);
       JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(tuple2);

       JavaPairRDD<String, Tuple2<Integer, Integer>> joinRdd = rdd1.join(rdd2);
       System.out.println(joinRdd.collect());
       JavaPairRDD<String, Integer> union = rdd1.union(rdd2);
       JavaPairRDD<String, Integer> res = union.reduceByKey(new Function2<Integer, Integer, Integer>() {
           @Override
           public Integer call(Integer integer, Integer integer2) throws Exception {
               return integer + integer2;
           }
       });
       System.out.println(union.collect());
       System.out.println(res.collect());
   }


    /**
     * spark -------cogroup
     */
   @Test
    public void SparkCoGroupDemo(){
       SparkConf conf = new SparkConf().setAppName("SparkCoGroupDemo").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       List<Tuple2<String, Integer>> tuple1 = new ArrayList<>();
       tuple1.add(new Tuple2<>("a",1));
       tuple1.add(new Tuple2<>("b",32));
       tuple1.add(new Tuple2<>("a",16));
       tuple1.add(new Tuple2<>("r",8));
       tuple1.add(new Tuple2<>("c",10));
       tuple1.add(new Tuple2<>("w",12));
       tuple1.add(new Tuple2<>("b",13));
       tuple1.add(new Tuple2<>("c",6));

       List<Tuple2<String, Integer>> tuple2 = new ArrayList<>();
       tuple2.add(new Tuple2<>("a",23));
       tuple2.add(new Tuple2<>("b",12));
       tuple2.add(new Tuple2<>("c",55));
       tuple2.add(new Tuple2<>("r",8));
       tuple2.add(new Tuple2<>("f",10));
       tuple2.add(new Tuple2<>("c",43));
       tuple2.add(new Tuple2<>("q",13));
       tuple2.add(new Tuple2<>("a",40));

       JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(tuple1);
       JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(tuple2);
       JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = rdd1.cogroup(rdd2);
       System.out.println(cogroup.collect());
       //[(a,([1, 16],[23, 40])), (q,([],[13])), (b,([32, 13],[12])), (r,([8],[8])), (c,([10, 6],[55, 43])), (f,([],[10])), (w,([12],[]))]20/08/13 11:09:31 INFO SparkContext: Invoking stop() from shutdown hook

   }

   @Test
    public void SparkReadMysql(){
       SparkConf conf = new SparkConf().setAppName("SparkReadMysql").setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       SQLContext sqlContext = new SQLContext(sc);
       //读取MySQL数据
       readMySQL(sqlContext);

   }

    private static void readMySQL(SQLContext sqlContext){
        //jdbc.url=jdbc:mysql://localhost:3306/database
        String url = "jdbc:mysql://localhost:3306/testdata";
        //查找的表名
        String table = "datataskinfor_bak";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取test数据库中的user_test表内容");
        // 读取表中所有数据
//        DataFrame jdbcDF = sqlContext.read().jdbc(url,table,connectionProperties).select("*");
        //显示数据
//        jdbcDF.show();
//        sqlContext.read().jdbc(url,table,connectionProperties).select("*").show();
    }


    /**
     * spark ------mapPartitions
     * mapPartitions函数会对每个分区依次调用分区函数处理，然后将处理的结果(若干个Iterator)生成新的RDDs。
     mapPartitions与map类似，但是如果在映射的过程中需要频繁创建额外的对象，使用mapPartitions要比map高效的过。
     比如，将RDD中的所有数据通过JDBC连接写入数据库，如果使用map函数，可能要为每一个元素都创建一个connection，
     这样开销很大，如果使用mapPartitions，那么只需要针对每一个分区建立一个connection
     */
    @Test
    public void SparkMapPartitions(){
        SparkConf conf = new SparkConf().setAppName("SparkMapPartitions").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lists = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
        JavaRDD<Integer> dataRdd = sc.parallelize(lists, 2);
        JavaRDD<Integer> resRdd = dataRdd.mapPartitions(
                new FlatMapFunction<Iterator<Integer>, Integer>() {
                    @Override
                    public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                        int sum = 0;
                        List<Integer> join = new ArrayList<>();
                        while (integerIterator.hasNext()) {
                            Integer next = integerIterator.next();
                            join.add(next);
                            sum += next;
                        }
                        List<Integer> res = new ArrayList<>();
                        res.add(sum);
                        int partitionId = TaskContext.getPartitionId();
//                        System.out.println("当前分区为：" + partitionId + ";其值为：" + StringUtils.join(join,","));
                        return res.iterator();
                    }
                }
        );

        List<Integer> collect = resRdd.collect();
        FileUtils.deleteQuietly(new File("C:\\Users\\admin\\Desktop\\sparktest\\c"));
        resRdd.saveAsTextFile("C:\\Users\\admin\\Desktop\\sparktest\\c");
        System.out.println(collect);
    }


    @Test
    public void SparkMapp(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        SparkContext sc = SparkContext.getOrCreate(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        List<String> list = Arrays.asList("1,2,3,4,5,6,7,8,9".split(","));
        JavaRDD<String> dataRdd = jsc.parallelize(list,3);
        JavaRDD<Integer> resultRdd = dataRdd.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<String> v) throws Exception {
                Integer sum = 0;
                ArrayList<Integer> res = new ArrayList<>();
                ArrayList<Integer> num = new ArrayList<>();
                while (v.hasNext()) {
                    String next = v.next();
                    int value = Integer.parseInt(next);
                    num.add(value);
                    sum += value;
                }
                res.add(sum);
                System.err.println(TaskContext.getPartitionId() +"::" + StringUtils.join(num,"/")+ ":::" + sum);
                return res.iterator();
            }
        });
        System.out.println(resultRdd.collect());
    }

/*标记：20201014*/
    /**
     * spark  ----------------------mapPartitionsWithIndex
     * 结果：[0|10, 0|21, 0|42, 1|30, 1|51, 1|62, 1|73]
     */
    @Test
    public void SparkMapPartitionsWithIndex(){
        SparkConf conf = new SparkConf().setAppName("SparkMapPartitionsWithIndex").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
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
        System.out.println(resRdd.collect());
    }


    /**
     * spark ------intersection
     * 统计两个算子的交集
     */
    @Test
    public void  SparkIntersection(){
        SparkConf conf = new SparkConf().setAppName("SparkIntersection").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Integer> list2 = Arrays.asList(21, 2, 0, 4, 5, 6, 7, 65, 9);
        JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        JavaRDD<Integer> rdd2 = sc.parallelize(list2);
        JavaRDD<Integer> res = rdd1.intersection(rdd2);
        JavaRDD<Integer> subtract = rdd1.subtract(rdd2);
        System.out.println(res.collect());
        System.out.println(subtract.collect());

    }


    /**
     * spark ---------CoalCase()
     * coalesce() 可以将 parent RDD 的 partition 个数进行调整，比如从 5 个减少到 3 个，
     * 或者从 5 个增加到 10 个。需要注意的是当 shuffle = false 的时候，
     * 是不能增加 partition 个数的（即不能从 5 个变为 10 个）。
     */
    @Test
    public void SparkColCaseAndRepartition(){
        SparkConf conf = new SparkConf().setAppName("SparkColCaseAndRepartition").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> rdd1 = sc.parallelize(list, 6);
        int numPartitions = rdd1.getNumPartitions();
        System.out.println("rdd1:" + numPartitions);
        JavaRDD<Integer> rdd2 = rdd1.coalesce(3);
        System.out.println("rdd2:"+ rdd2.getNumPartitions());
        JavaRDD<Integer> rdd3 = rdd2.coalesce(5, false);//如果将第二个参数改为true,最后的结果就会是5个分区
        System.out.println("rdd3:" + rdd3.getNumPartitions());
        /**
         * 结果：
         * rdd1:6
         * rdd2:3
         * rdd3:3
         */
    }

    /**
     * spark --------repartition
     * 特别需要说明的是，如果使用repartition对RDD的partition数目进行缩减操作，
     * 可以使用coalesce函数，将shuffle设置为false，避免shuffle过程，提高效率。
     * 会产生shuffer
     */
    @Test
    public void SparkRepartition(){
        SparkConf conf = new SparkConf().setAppName("SparkColCaseAndRepartition").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> rdd1 = sc.parallelize(list, 6);
        int numPartitions = rdd1.getNumPartitions();
        System.out.println("rdd1:" + numPartitions);
        JavaRDD<Integer> rdd2 = rdd1.repartition(3);
        System.out.println("rdd2:"+ rdd2.getNumPartitions());
        JavaRDD<Integer> rdd3 = rdd2.repartition(7);
        System.out.println("rdd3:" + rdd3.getNumPartitions());
        /**
         * 结果：
         * rdd1:6
         * rdd2:3
         * rdd3:7
         */
    }


    /**
     * spark --------Cartesian
     * 对两个算子进行笛卡尔积统计
     */
    @Test
    public void SparkCartesian(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaPairRDD<Integer, Integer> res = rdd.cartesian(rdd);
        System.out.println(res.collect().size());
    }

    /**
     * spark ------Distinct进行去重
     */
    @Test
    public void SparkDistinct(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 5, 7, 9, 52, 6, 9, 4, 3, 2);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<Integer> rdd1 = rdd.distinct();
        System.out.println(rdd1.collect());
        JavaRDD<Integer> rdd2 = rdd.distinct(3);
        System.out.println(rdd2.collect());
    }



    @Test
    public void SparkAggregate() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String,Integer> javaPairRDD =
                sc.parallelizePairs(Lists.<Tuple2<String, Integer>>newArrayList(new Tuple2<String, Integer>("cat",34),
                        new Tuple2<String, Integer>("cat",34),new Tuple2<String, Integer>("dog",20),new Tuple2<String, Integer>("tiger",34)),2);

        // 打印样例数据

        javaPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("样例数据>>>>>>>" + stringIntegerTuple2);
            }
        });
        Integer integer = javaPairRDD.aggregate(0, new Function2<Integer, Tuple2<String, Integer>, Integer>() {
            public Integer call(Integer v1, Tuple2<String, Integer> v2) throws Exception {

                System.out.println("seqOp>>>>>  参数One："+v1+"--参数Two:"+v2);
                return v1+v2._2();
            }
        }, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("combOp>>>>>  参数One："+v1+"--参数Two:"+v2);
                return v1+v2;
            }
        });
        System.out.println("result:"+integer);
    }


    @Test
    public void SparkLeftJoin(){
        SparkConf conf = new SparkConf().setAppName("SparkJoinDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> tuple1 = new ArrayList<>();
        tuple1.add(new Tuple2<>("a",1));
        tuple1.add(new Tuple2<>("b",32));
        tuple1.add(new Tuple2<>("c",45));
        tuple1.add(new Tuple2<>("r",8));
        tuple1.add(new Tuple2<>("f",54));
        tuple1.add(new Tuple2<>("w",12));
        tuple1.add(new Tuple2<>("q",13));
        tuple1.add(new Tuple2<>("j",6));

        List<Tuple2<String, Integer>> tuple2 = new ArrayList<>();
        tuple2.add(new Tuple2<>("a",2));
        tuple2.add(new Tuple2<>("p",32));
        tuple2.add(new Tuple2<>("c",16));
        tuple2.add(new Tuple2<>("o",8));
        tuple2.add(new Tuple2<>("f",10));
        tuple2.add(new Tuple2<>("s",12));
        tuple2.add(new Tuple2<>("n",13));
        tuple2.add(new Tuple2<>("m",6));

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(tuple1);
        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(tuple2);
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> resRdd = rdd1.leftOuterJoin(rdd2);
        System.out.println("leftjoin"+resRdd.collect());

        JavaPairRDD<String, Integer> intersection = rdd1.intersection(rdd2);
        System.out.println("inner :"+intersection.collect());

        JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rightRdd = rdd1.rightOuterJoin(rdd2);
        System.out.println("right : "+rightRdd.collectAsMap());;

    }

    /**
     * sortByKey的使用
     */
    @Test
    public void SparkSortByKey(){
        SparkConf conf = new SparkConf().setAppName("SparkSortByKey").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 3, 2, 7, 4, 5, 9, 7, 12, 11);
        JavaRDD<Integer> rdd1 = sc.parallelize(list);
        JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(f -> {
            return new Tuple2<>(f, new Random().nextInt(100));
        });

        JavaPairRDD<Integer, Integer> cache = rdd2.cache();
        System.err.println(cache.collect());
        JavaPairRDD<Integer, Integer> sortRdd = cache.sortByKey();
        System.err.println(sortRdd.collect());
        JavaPairRDD<Integer, Integer> sortRfdd2 = cache.sortByKey(false);
        System.err.println(sortRfdd2.collect());
        System.out.println(sortRfdd2.take(3));
    }

    /**
     * sortBy的使用
     */
    @Test
    public void SparkSortBy(){
        SparkConf conf = new SparkConf().setAppName("SparkSortByKey").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 3, 2, 7, 4, 5, 9, 7, 12, 11);
        JavaRDD<Integer> rdd1 = sc.parallelize(list);
        JavaRDD<String> map = rdd1.map(f -> {
            return f + "_" + new Random().nextInt(100);
        });

        JavaRDD<String> cache = map.cache();
        JavaRDD<String> resRdd = cache.sortBy(
                new Function<String, Integer>() {
                    @Override
                    public Integer call(String v1) throws Exception {
                        return Integer.parseInt(v1.split("_")[0]);
                    }
                },
                false,
                2
        );
        System.out.println(resRdd.collect());

        JavaRDD<String> stringJavaRDD = cache.sortBy(f -> {
            String[] split = f.split("_");
            String s = split[1];
            return s;
        }, false, 2);
        System.out.println("lambda::::" +stringJavaRDD.collect());
    }

    /**
     * repartitionAndSortWithinPartitions的使用
     */
    @Test
    public void SparkrepartitionAndSortWithinPartitions(){
        SparkConf conf = new SparkConf().setAppName("SparkSortByKey").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 3, 2, 7, 4, 5, 9, 7, 12, 11);
        JavaRDD<Integer> dataRdd = sc.parallelize(list);
        JavaPairRDD<Integer, Integer> rdd1 = dataRdd.mapToPair(f -> {
            return new Tuple2<>(f, new Random().nextInt(200));
        });
        JavaPairRDD<Integer, Integer> rdd2 = rdd1.repartitionAndSortWithinPartitions(
                new Partitioner() {
                    @Override
                    public int getPartition(Object key) {
                        return key.toString().hashCode() % numPartitions();
                    }

                    @Override
                    public int numPartitions() {
                        return 3;
                    }
                }
        );
        System.out.println(rdd2.getNumPartitions());
        System.err.println(rdd2.collect());
    }

    /**
     * CombinerByKey的使用
     */
    @Test
    public void SparkCombinnerByKey() {
        SparkConf conf = new SparkConf().setAppName("CombineByKey").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> l1 = new ArrayList<>();
        l1.add("dog");
        l1.add("cat");
        l1.add("gnu");
        l1.add("salmon");
        l1.add("rabbit");
        l1.add("turkey");
        l1.add("wolf");
        l1.add("bear");
        l1.add("bee");
        JavaRDD<String> javaRDD = jsc.parallelize(l1, 3);
        JavaRDD<Integer> javaRDD2 = jsc.parallelize(Arrays.asList(1, 1, 2, 2, 2, 1, 2, 2, 2), 3);
        JavaPairRDD<Integer, String> javaPairRDD = javaRDD2.zip(javaRDD);
        System.out.println(javaPairRDD.collect());
        JavaPairRDD<Integer, List<String>> resRdd = javaPairRDD.combineByKey(
                new Function<String, List<String>>() {
                    @Override
                    public List<String> call(String v1) throws Exception {
                        List<String> list = new ArrayList<>();
                        list.add(v1);
                        return list;

                    }
                },
                new Function2<List<String>, String, List<String>>() {
                    @Override
                    public List<String> call(List<String> v1, String v2) throws Exception {
                        v1.add(v2);
                        return v1;
                    }
                },
                new Function2<List<String>, List<String>, List<String>>() {
                    @Override
                    public List<String> call(List<String> v1, List<String> v2) throws Exception {
                        v1.addAll(v2);
                        return v1;
                    }
                }
        );

        System.out.println(resRdd.collect());
    }

    /**
     * reduceByKey的使用
     */
    @Test
    public void SparkReduceByKey(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 4, 2, 3, 5, 7, 1, 3, 2, 5, 1);
        JavaRDD<Integer> rdd1 = sc.parallelize(list);
        JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(f -> {
            return new Tuple2<>(f, 1);
        });
        JavaPairRDD<Integer, Integer> resRdd = rdd2.reduceByKey((f1, f2) -> {
            return f1 + f2;
        }, 2);



        System.out.println(resRdd.collect());
    }

//20200825标记
    /**
     * foldByKey的使用
     */
    @Test
    public void SparkFoldByKey(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 4, 2, 3, 5, 7, 1, 3, 2, 5, 1);
        JavaRDD<Integer> rdd1 = sc.parallelize(list);
        JavaPairRDD<Integer, String> pairRdd = rdd1.mapToPair(f -> {
            return new Tuple2<>(f, Integer.toString(new Random().nextInt(100)));
        }).cache();
        System.err.println(pairRdd.collect());
        JavaPairRDD<Integer, String> resRdd = pairRdd.foldByKey("x",
                (f1, f2) -> {
                    return f1 + ":" + f2;
                }
        );
        System.err.println(resRdd.collect());
    }


    @Test
    public void SparkFoldByKey1(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 4, 2, 3, 5, 7, 1, 3, 2, 5, 1);
        JavaRDD<Integer> rdd1 = sc.parallelize(list);
        JavaPairRDD<Integer, String> pairRdd = rdd1.mapToPair(f -> {
            return new Tuple2<>(f, Integer.toString(new Random().nextInt(100)));
        });

        JavaPairRDD<Integer, String> resRdd = pairRdd.foldByKey("x",
                new Partitioner() {
                    @Override
                    public int getPartition(Object key) {
                        return key.toString().hashCode() % numPartitions();
                    }

                    @Override
                    public int numPartitions() {
                        return 2;
                    }
                }, new Function2<String, String, String>() {
                    @Override
                    public String call(String v1, String v2) throws Exception {
                        return v1 + v2;
                    }
                });
        System.out.println(resRdd.collect());
    }

    @Test
    public void SparkZip(){
        SparkConf conf = new SparkConf().setAppName("CombineByKey").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> l1 = new ArrayList<>();
        l1.add("dog");
        l1.add("cat");
        l1.add("gnu");
        l1.add("salmon");
        l1.add("rabbit");
        l1.add("turkey");
        l1.add("wolf");
        l1.add("bear");
        l1.add("bee");
        JavaRDD<String> javaRDD = jsc.parallelize(l1, 3);
        JavaRDD<Integer> javaRDD2 = jsc.parallelize(Arrays.asList(1, 1, 2, 2, 2, 1, 2, 2, 2), 3);
        JavaPairRDD<Integer, String> javaPairRDD = javaRDD2.zip(javaRDD);
        System.out.println(javaPairRDD.collect());
    }

    /**
     *
     * zipPartitions的使用
     */
    @Test
    public void SparkzipPartitions(){
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        List<Integer> data1 = Arrays.asList(3, 2, 12, 5, 6, 1);
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> dataRdd1 = sc.parallelize(data,2);
        JavaRDD<Integer> dataRdd2 = sc.parallelize(data1,2);
        JavaRDD<String> resRdd = dataRdd1.zipPartitions(dataRdd2,
                new FlatMapFunction2<Iterator<Integer>, Iterator<Integer>, String>() {
                    @Override
                    public Iterator<String> call(Iterator<Integer> integerIterator, Iterator<Integer> integerIterator2) throws Exception {
                        LinkedList<String> ss = new LinkedList<>();
                        while (integerIterator.hasNext() && integerIterator2.hasNext()) {
                            ss.add(integerIterator.next() + "_" + integerIterator2.next());
                        }
                        return ss.iterator();
                    }
                });

        System.err.println(resRdd.collect());
    }

    /**
     * ZipWithIndex,zipWithUniqueId的使用
     */
    @Test
    public void SparkZipWithIndex(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = sc.parallelize(data,3);
        List<Integer> data1 = Arrays.asList(3,2,12,5,6,1,7);
        JavaRDD<Integer> javaRDD1 = sc.parallelize(data1);
        JavaPairRDD<Integer,Long> zipWithIndexRDD = javaRDD.zipWithIndex();
        JavaPairRDD<Integer, Long> resRdd2 = javaRDD1.zipWithUniqueId();


        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + zipWithIndexRDD.collect());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + resRdd2.collect());

    }


    /**
     * action 算子的练习
     */

    @Test
    public void SparkActionReduce(){
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        Integer res = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(res);
    }

    /**
     * aggregate的使用
     */
    @Test
    public void SparkActionAggregate(){
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        Integer res = rdd.aggregate(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(res);

    }


    @Test
    public void SparkActionFlod(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        Integer resRdd = rdd.fold(0,
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        System.out.println(resRdd);
        Integer fold = rdd.fold(10,
                (f1, f2) -> {
                    return f1 + f2;
                });

        System.out.println(fold);
    }

    @Test
    public void SparkActionCountByKey(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        JavaPairRDD<Integer, Integer> mapRdd = rdd.mapToPair(f -> {
            return new Tuple2<>(f, 1);
        });

        Map<Integer, Long> res = mapRdd.countByKey();
        System.out.println(res);
    }

    /**
     * foreach ,foreachPartition，lookup的使用
     */
    @Test
    public void SparkActionForeach(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> rdd = sc.parallelize(data,3);
//        rdd.foreach(f ->{
//
//            System.out.println(f);
//        });
        rdd.foreachPartition(f ->{
            List<Integer> list = new ArrayList<>();
            while(f.hasNext()){
                list.add(f.next());
            }
            System.out.println(TaskContext.getPartitionId()+":" + StringUtils.join(list,","));


        });


        JavaPairRDD<Integer, Integer> lookupRdd = rdd.mapToPair(f -> {
            return new Tuple2<>(f, new Random().nextInt(100));
        });
        JavaPairRDD<Integer, Integer> res = lookupRdd.cache();
        System.out.println(res.collect());
        List<Integer> lookup = res.lookup(4);
        System.err.println(lookup);
    }


    public static class TakeOrderedComparator implements Serializable,Comparator<Integer>{
        @Override
        public int compare(Integer o1, Integer o2) {
            return -o1.compareTo(o2);
        }
    }

    @Test
    public void SparkTableOrdered(){
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(1, 2, 3, 9, 43,1346, 4, 7, 8, 5));
        List<Integer> re = dataRdd.takeOrdered(3, new TakeOrderedComparator());
        System.err.println(re);
        List<Integer> top = dataRdd.top(3);
        System.err.println("top:" + top);
    }
/*20200914标记*/
    /**
     * 检查点（checkpoint机制）
     */
    @Test
    public void SparkCheckPoint(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkCheckPoint");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String checkpointdir = "C:\\Users\\admin\\Desktop\\sparktest\\checkpoint";
        sc.setCheckpointDir(checkpointdir);
        //统计worldcount的结果
        String  inpath = "C:\\Users\\admin\\Desktop\\sparktest\\a.txt";
        JavaRDD<String> rdd1 = sc.textFile(inpath);
        JavaRDD<String> flatmap = rdd1.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        String[] split = s.split(" ");
                        List<String> list = Arrays.asList(split);
                        return list.iterator();
                    }
                }
        );

        JavaPairRDD<String, Integer> pairRdd = flatmap.mapToPair(f -> {
            return new Tuple2<>(f, 1);
        });

        JavaPairRDD<String, Integer> resRdd = pairRdd.reduceByKey((f1, f2) -> {
            return f1 + f2;
        });
        resRdd.checkpoint();
        String outpath = "C:\\Users\\admin\\Desktop\\sparktest\\wordcount";
        FileUtils.deleteQuietly(new File(outpath));
        resRdd.saveAsTextFile(outpath);
        System.out.println(resRdd.collect());
    }


    /**
     * 广播变量的使用（Broadcast）
     * 累加器的使用（Accumulator）
     */
    @Test
    public  void SparkBroadcast(){
        SparkConf conf = new SparkConf().setAppName("SparkBroadcast").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("a", "c", "d", "f","e");
        //定义广播变量
        Broadcast<List<String>> broadcast = sc.broadcast(list);
        //累加器的使用
//        Accumulator<Integer> accumulator = sc.accumulator(0);
        LongAccumulator accumulator = sc.sc().longAccumulator();
        CollectionAccumulator<Object> mapAccumulator = sc.sc().collectionAccumulator("集合累加器");

        String inpath = "C:\\Users\\admin\\Desktop\\sparktest\\a.txt";
        JavaRDD<String> dataRdd = sc.textFile(inpath);
        JavaRDD<String> flatMapRdd = dataRdd.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        String[] str = s.split(" ");
                        List<String> list = Arrays.asList(str);
                        return list.iterator();
                    }
                }
        );

        JavaRDD<String> filterRdd = flatMapRdd.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {
                        List<String> value = broadcast.value();
                        if (value.contains(v1)) {
                            accumulator.add(1);
                            return true;
                        }
                        return false;
                    }
                }
        );
        JavaPairRDD<String, Integer> pairRdd = filterRdd.mapToPair(f -> {
            return  new Tuple2<>(f,1);

        });

        JavaPairRDD<String, Integer> resRdd = pairRdd.reduceByKey((f1, f2) -> {
            return f1 + f2;
        });

        System.out.println(resRdd.collect());
        System.out.println("tongji:"+accumulator.value());
        System.out.println(mapAccumulator.value());
    }

    /**
     * 累加器的使用（Accumulator）
     */
//    @Test
//    public void SparkAccumulator(){
//        SparkConf conf = new SparkConf().setAppName("SparkAccumulator").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> dataRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\a.txt",2);
//        JavaRDD<String> resRdd = dataRdd.flatMap(f -> {
//            String[] split = f.split(" ");
//            List<String> list = Arrays.asList(split);
//            return list.iterator();
//        });
//        MapAccumulator mapAccumulator = new MapAccumulator();
//        sc.sc().register(mapAccumulator,"集合累加器");
//        /**
//         *  rdd.foreachPartition(f ->{
//         List<Integer> list = new ArrayList<>();
//         while(f.hasNext()){
//         list.add(f.next());
//         }
//         System.out.println(TaskContext.getPartitionId()+":" + StringUtils.join(list,","));
//
//
//         });
//         */
//        resRdd.foreachPartition(
//              f ->{
//                  List<String> list = new ArrayList<>();
//                  while(f.hasNext()){
//                      String value = f.next();
//                      mapAccumulator.add(value);
//                      list.add(value);
//                      System.out.println(TaskContext.getPartitionId()+":" + StringUtils.join(list,","));
//
//                  }
//              }
//        );
//
//        Map value = mapAccumulator.value();
//        System.out.println(value);
//
//    }


    /**
     * pv统计前五的数据
     */
    @Test
    public void SparkPVUVDemo(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkPVUVDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        JavaRDD<String> dataRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\pv_uv.txt");
//        System.out.println(dataRdd.collect());
        JavaPairRDD<String, Integer> pairRDD = dataRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split("\t");
                String s1 = split[5];
                return new Tuple2<>(s1, 1);
            }
        });

        JavaPairRDD<String, Integer> resRdd = pairRDD.reduceByKey((f1, f2) -> {
            return f1 + f2;
        });

        List<Tuple2<String, Integer>> take = resRdd.sortByKey().take(5);
        System.out.println(take);

    }
    @Test
    public void SparkUV(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("uv");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> dataRdd = sc.textFile("C:\\Users\\admin\\Desktop\\sparktest\\pv_uv.txt");
        JavaPairRDD<String, Integer> pairRDD = dataRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split("\t");
                String ss = split[0] + "_" + split[4] + "_" + split[5];
                return new Tuple2<>(ss, 1);
            }
        });


        JavaPairRDD<String, Integer> pairRDD1 = pairRDD.distinct().mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                String s = stringIntegerTuple2._1;
                String[] split = s.split("_");

                return new Tuple2<>(split[2], 1);
            }
        });

        JavaPairRDD<String, Integer> pairRDD2 = pairRDD1.reduceByKey((f1, f2) -> {
            return f1 + f2;
        });
        List<Tuple2<String, Integer>> take = pairRDD2.sortByKey().take(3);
        System.err.println(take);


    }

    /**
     * 根据spark-core获取MySQL的数据
     * JdbcRdd.create(),
     */
    @Test
    public void SparkCoreReadMysql(){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        Map<String, String> map = new HashMap<>();
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("url","jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
        map.put("user","root");
        map.put("password","");
        map.put("dbtable","datataskinfor_bak");
        String sql = "select dataTaskId,dataTaskName,taskType,searchType,searchField,searchFrom from datataskinfor_bak where dataTaskId >=? and dataTaskId <=?";
        JavaRDD<String> javaRDD = JdbcRDD.create(jsc, new JdbcRDD.ConnectionFactory() {
            @Override
            public Connection getConnection() throws Exception {
                Class.forName(map.get("driver")).newInstance();

                return DriverManager.getConnection(map.get("url"), map.get("user"), map.get("password"));
            }
        }, sql, 400, 414, 1, new Function<ResultSet, String>() {
            @Override
            public String call(ResultSet rs) throws Exception {
                int dataTaskId = rs.getInt("dataTaskId");
                String dataTaskName = rs.getString("dataTaskName");
                int taskType = rs.getInt("taskType");
                int searchType = rs.getInt("searchType");
                String searchField = rs.getString("searchField");
                Timestamp searchFromTime = rs.getTimestamp("searchFrom");
                String searchFrom = getDate(searchFromTime);
                return "[" + dataTaskId + "," + dataTaskName + "," + taskType + "," + searchType + "," + searchField + "," + searchFrom + "]";
            }
        });

        javaRDD.foreach( f ->{
            System.out.println(f);
        });
    }

    //指定时间戳的输出格式
    public static String getDate(Timestamp timestamp){
        String format = "";
        if(timestamp != null){
            long time = timestamp.getTime();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            format = sdf.format(new Date(time));
        }else{
            format = null;
        }
        return format;
    }
}
