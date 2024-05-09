package hadoop.mapreduce.questions;

import org.apache.spark.api.java.JavaRDD;

public interface Question {

    void answer(JavaRDD<String> lines);
}
