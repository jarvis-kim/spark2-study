package kr.co.jarvisk.study.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RddTest {

    private JavaSparkContext sc;

    @Before
    public void init() {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStudy");
        sc = new JavaSparkContext(sparkConf);
    }


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

    /**
     * 파티션 3개 생성.
     * 파티션마다 돔.
     */
    @Test
    public void testMapPartitions() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

        JavaRDD<Integer> rdd2 = rdd1.mapPartitions(integerIterator -> {
            System.out.println("Enter Partition!");
            List<Integer> numbers = new ArrayList<>();
            integerIterator.forEachRemaining(integer -> numbers.add(integer + 1));
            return numbers.iterator();
        });

        System.out.println(rdd2.collect());
    }
}
