package org.bobby.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 多文件读取
 * <p>
 * 例子如下，被索引的文件为（0，1，2 代表文件名）
 * <p>
 * “it is what it is”
 * “what is it”
 * “it is a banana”
 * 我们就能得到下面的反向文件索引：
 * “a”: {2}
 * “banana”: {2}
 * “is”: {0, 1, 2}
 * “it”: {0, 1, 2}
 * “what”: {0, 1}
 * 再加上词频为：
 * “a”: {(2,1)}
 * “banana”: {(2,1)}
 * “is”: {(0,2), (1,1), (2,1)}
 * “it”: {(0,2), (1,1), (2,1)}
 * “what”: {(0,1), (1,1)}`
 */
public class IndexProcessor {
    /**
     * 入口
     *
     * @param args 入参
     */
    public static void main(String[] args) {
        // configuration
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        //读取目录docs并打印
        JavaPairRDD<String, String> data = javaSparkContext.wholeTextFiles("src/main/resources/docs/", 1);
        List<Tuple2<String, String>> tuple2s = data.collect();
        for (Tuple2<String, String> tup : tuple2s) {
            System.out.println("* " + tup);
        }

        JavaPairRDD<String, String> dd = data.flatMap((FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>) tuple -> {
            String[] docPaths = tuple._1.split("/");
            String docName = Arrays.stream(docPaths[docPaths.length - 1].split("\\.")).findFirst().orElse("");
            return Arrays.stream(tuple._2.split("\\n"))
                    .flatMap(word -> Arrays.stream(word.split(" ")).map(w -> Tuple2.apply(w, docName))).iterator();
        }).mapToPair((PairFunction<Tuple2<String, String>, Tuple2<String, String>, Long>) stringStringTuple2 -> Tuple2.apply(stringStringTuple2, 1L))
                .reduceByKey((Function2<Long, Long, Long>) Long::sum)
                .mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Long>, String, String>) tuple2LongTuple2 ->
                        Tuple2.apply(tuple2LongTuple2._1._1, String.format("(%s,%s)",tuple2LongTuple2._1._2, tuple2LongTuple2._2.toString())))
                .reduceByKey((Function2<String, String, String>) (v1, v2) -> String.format("{%s,%s}", v1, v2));
        for (Tuple2<String, String> tuple : dd.collect()) {
            System.out.println("----- " + tuple.toString());
        }
        JavaRDD<String> output = dd.map((Function<Tuple2<String, String>, String>) v1 -> String.format("\"%s\": %s", v1._1, v1._2));
        System.out.println(output.collect().toString());
    }
}
