package com.nisum;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
       SparkSession session=SparkSession
                            .builder()
                            .appName("Sparkwithjava")
                            .master("local[3]")
                            .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(session.sparkContext());
        List<Integer> integerList= Arrays.asList(1,2,3,4);

        try {
            JavaRDD<Integer> parallelize = context.parallelize(integerList, 3);
            parallelize.foreach(integer->System.out.println("Java RDD"+integer));
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        context.stop();
    }
}
