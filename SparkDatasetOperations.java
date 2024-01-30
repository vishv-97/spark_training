import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class SparkDatasetOperations {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkDatasetOperations")
                .master("local[*]")
                .getOrCreate();

        // Task 1: Create a list using the provided data
        List<Row> data = Arrays.asList(
                RowFactory.create("Joe", "Smith", "M", 45),
                RowFactory.create("Jack", "Miller", "M", 23),
                RowFactory.create("Anna", "Wood", "F", 32)
        );

        // Task 2: Convert the list into a DataFrame adhering to the specified schema
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("firstName", DataTypes.StringType, true),
                DataTypes.createStructField("lastName", DataTypes.StringType, true),
                DataTypes.createStructField("Gender", DataTypes.StringType, true),
                DataTypes.createStructField("AgeInInteger", DataTypes.IntegerType, true)
        );

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Task 3: Display the schema of the resulting DataFrame
        df.printSchema();

        // Task 4: Filter individuals whose age ranges from 1 to 50
        Dataset<Row> filteredDF = df.filter("AgeInInteger >= 1 AND AgeInInteger <= 50");

        // Task 5: Add a new column named isYoung which evaluates to True for ages under 40, and False otherwise
        df = df.withColumn("isYoung", functions.when(df.col("AgeInInteger").lt(40), true).otherwise(false));

        // Task 6: Calculate the average age of people based on their gender
        Dataset<Row> avgAgeByGender = df.groupBy("Gender").agg(functions.avg("AgeInInteger").as("AverageAge"));

        // Display results
        System.out.println("Filtered DataFrame:");
        filteredDF.show();

        System.out.println("DataFrame with isYoung column:");
        df.show();

        System.out.println("Average age by gender:");
        avgAgeByGender.show();

        spark.stop();
    }
}

