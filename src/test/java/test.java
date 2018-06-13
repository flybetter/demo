import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * test class
 *
 * @author bingyu wu
 *         Date: 2018/6/11
 *         Time: 下午4:54
 */
public class test {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        String logFile = "hdfs://192.168.105.21:9000/Hadoop/Input/wordcount.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("spark://192.168.105.21:7077")
                .setJars(new String[]{"/Users/wubingyu/IdeaProjects/365company_demo/demo/target/demo-1.0-SNAPSHOT.jar"});
        //注意设置jar包路径，以免报找不到class的异常
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(logFile).cache();
//        JavaRDD<String> lines = sc.textFile(args[0]).cache();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String s) throws Exception {

                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });


        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?, ?> tuple : output) {

            System.out.println(tuple._1 + ":" + tuple._2);
        }

        sc.close();
    }

//        long numAs = logData.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) {
//                return s.contains("a");
//            }
//        }).count();
//
//        long numBs = logData.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) {
//                return s.contains("b");
//            }
//        }).count();
//
//        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
//
//        sc.stop();
//    }
}
