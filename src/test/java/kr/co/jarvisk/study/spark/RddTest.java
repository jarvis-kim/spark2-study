package kr.co.jarvisk.study.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RddTest {

    private JavaSparkContext sc;

    @Before
    public void init() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStudy");
        sc = new JavaSparkContext(sparkConf);
    }

    /* --- Map Operation */

    @Test
    public void testMap() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<String> rdd2 = rdd1.map(value -> value + "-str");
        System.out.println(rdd2.collect());
    }

    @Test
    public void testFlatMap() {
        List<String> data = Arrays.asList("apple,orange", "grape,apple,mango", "blueberry,tomato,orange");

        JavaRDD<String> rdd1 = sc.parallelize(data);
        JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split(",")).iterator());
        List<String> result = rdd2.collect();
        System.out.println(result.size());
        System.out.println(result);

//        List<String[]> arrayData = rdd1.map(s -> s.split(",")).collect();
//        arrayData.stream().forEach(array -> System.out.println(array));
    }


    /* --- Partition Operation */

    /**
     * 파티션 3개 생성.
     * 파티션마다 돔.
     */
    @Test
    public void testMapPartitions() {
        JavaRDD<Integer> rdd1 = sc.parallelize(IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()), 3);

        JavaRDD<Integer> rdd2 = rdd1.mapPartitions(integerIterator -> {
            System.out.println("Enter Partition!");
            List<Integer> numbers = new ArrayList<>();
            integerIterator.forEachRemaining(integer -> numbers.add(integer + 1));
            return numbers.iterator();
        });

        System.out.println(rdd2.collect());
    }

    @Test
    public void testMapPartitionsWithIndex() {
        JavaRDD<Integer> rdd1 = sc.parallelize(IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()), 3);
        JavaRDD<Integer> rdd2 = rdd1.mapPartitionsWithIndex((Integer index, Iterator<Integer> numbers) -> {
            List<Integer> result = new ArrayList<>();
            if ( index == 1 ) {
                numbers.forEachRemaining(result::add);
            }

            return result.iterator();
        }, true);

        System.out.println(rdd2.collect());
    }


    /* -- Pair Operation */
    @Test
    public void testMapValues() {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair((String t) -> new Tuple2<>(t, 1))
                .mapValues((Integer v1) -> v1 + 1);

        System.out.println(rdd2.collect());
    }

    @Test
    public void testFlatMapValues() {
        JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2(1, "a,b"), new Tuple2(2, "a,c"), new Tuple2(3, "d,e")));
        JavaPairRDD<Integer, String> rdd2 = rdd1.flatMapValues((String s) -> Arrays.asList(s.split(",")));
        System.out.println(rdd2.collect());
    }


    /* Group Operation */

    @Test
    public void testZip() {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3));

        JavaPairRDD<String, Integer> rdd3 = rdd1.zip(rdd2);
        System.out.println(rdd3.collect());
    }

    @Test
    public void testZipPartitions() {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"), 2);
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3), 2);

        JavaRDD<String> rdd3 = rdd1.zipPartitions(rdd2, (Iterator<String> stringIterator, Iterator<Integer> integerIterator) -> {
            List<String> list = new ArrayList<>();
            stringIterator.forEachRemaining(s -> {
                System.out.println("stringIterator.forEachRemaining : " + s);
                integerIterator.forEachRemaining(i -> {
                    System.out.println("integerIterator.forEachRemaining : " + i);
                    list.add(s + i);
                });
            });

            return list.iterator();
        });

        System.out.println(rdd3.collect());
    }


    @Test
    public void testGroupBy() {
        JavaRDD<Integer> rdd1 = sc.parallelize(IntStream.range(1, 11).boxed().collect(Collectors.toList()));
        JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupBy(v1 -> v1.intValue() % 2 == 0 ? "even" : "odd");
        System.out.println(rdd2.collect());
    }


    @Test
    public void testGroupByKey() {
        List<Tuple2<String, Integer>> list = Arrays.asList("a", "b", "c", "b", "c").stream().map(s -> new Tuple2<>(s, 1)).collect(Collectors.toList());
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<Integer>> rdd2 =  rdd1.groupByKey();
        System.out.println(rdd2.collect());
    }

    @Test
    public void testCogroup() {
        JavaPairRDD<String, String> rdd1= sc.parallelize(Arrays.asList("k1", "k2", "k1")).zip(sc.parallelize(Arrays.asList("v1", "v2", "v3")));
        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("k1", "v4")));
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> rdd3 =  rdd1.cogroup(rdd2);

        System.out.println(rdd3.collect());
        System.out.println(rdd2.cogroup(rdd1).collect());

        JavaPairRDD<String, String> rdd4 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("k2", "v4")));
        System.out.println(rdd1.cogroup(rdd2, rdd4).collect());
    }

    @Test
    public void testDistinct() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 1, 3, 1, 2, 3));
        JavaRDD<Integer> rdd2 = rdd1.distinct();
        System.out.println(rdd2.collect());
    }

    @Test
    public void testCartesian() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaPairRDD<Integer, String> rdd3 = rdd1.cartesian(rdd2);
        System.out.println(rdd3.collect());
    }

    @Test
    public void testSubtract() {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("d", "e"));
        JavaRDD<String> rdd3 = rdd1.subtract(rdd2);

        System.out.println(rdd3.collect());
    }

    @Test
    public void testUnion() {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "c"));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("d", "e", "a", "f"));

        JavaRDD<String> rdd3 = rdd1.union(rdd2);

        System.out.println(rdd3.collect());
        System.out.println(rdd3.distinct().collect());
    }

    @Test
    public void testIntersection() {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "a", "b", "c"));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "a", "c", "c"));

        System.out.println(rdd1.intersection(rdd2).collect());
    }

    @Test
    public void join() {
        JavaPairRDD<String, Integer> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e")).mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> rdd2 = sc.parallelize(Arrays.asList("b", "c")).mapToPair(s -> new Tuple2<>(s, 2));

        JavaPairRDD<String, Tuple2<Integer, Integer>> rdd3 = rdd1.join(rdd2);
        System.out.println(rdd3.collect());
    }

    @Test
    public void testOuterJoin() {
        JavaPairRDD<String, Integer> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c")).mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> rdd2 = sc.parallelize(Arrays.asList("b", "c")).mapToPair(s -> new Tuple2<>(s, 2));

        System.out.println("Left Outer Join : " + rdd1.leftOuterJoin(rdd2).collect());
        System.out.println("Right Outer Join : " + rdd1.rightOuterJoin(rdd2).collect());
    }

    @Test
    public void testSubtractByKey() {
        JavaPairRDD<String, Integer> rdd1 = sc.parallelize(Arrays.asList("a", "b")).mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> rdd2 = sc.parallelize(Arrays.asList("b")).mapToPair(s -> new Tuple2<>(s, 2));

        JavaPairRDD<String, Integer> rdd3 = rdd1.subtractByKey(rdd2);
        System.out.println(rdd3.collect());
    }

}
