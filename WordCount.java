import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        String inputFile = "src/main/resources/input_file.txt";

        // I have use the spark.read().textFile() method to read the text file into a DataFrame.
        // The toDF("text") part assigns the name "text" to the column containing the text from the file.
        // Creating a DataFrame from the text file
        Dataset<Row> textDataFrame = spark.read().textFile(inputFile).toDF("text");


        // we use the selectExpr method to split the lines into words.
        // The explode(split(text, ' ')) expression uses the split function
        // to split the text into an array of words, and explode transforms
        // the array of words into separate rows for each word.
        // We then perform a grouping by the "word" column,
        // count the occurrences of each word, and sort the result
        // in descending order of count.
        // Split the lines into words using the flatMap transformation
        Dataset<Row> wordsDataFrame = textDataFrame
                .selectExpr("explode(split(text, ' ')) as word")
                .groupBy("word")
                .count()
                .sort(functions.col("count").desc());

        // Show the result
        wordsDataFrame.show();

        spark.stop();
    }
}

