import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class WuzzufJobsDAO {


    public SparkSession openSession(String sessionName,int local){

        return SparkSession.builder().appName (sessionName).master("local["+local+"]").getOrCreate();
    }

    public Dataset<Row> readCsvData(SparkSession sparkSession, String path,boolean head){

        // Get DataFrameReader using SparkSession.
        DataFrameReader dataFrameReader = sparkSession.read();

        // Set header option to true to specify that first row in file contains column name.
        dataFrameReader.option("header", head);
        return dataFrameReader.csv(path);
    }

    public void displayHead(SparkSession sparkSession, Dataset df,int number){
        // Create view and execute query to display first n rows of WuzzufJobs data:
        df.createOrReplaceTempView ("WuzzufJobs");
        sparkSession.sql("SELECT * FROM WuzzufJobs LIMIT "+ number+";").show();
    }
//
    public Dataset<Row> sqlQuery(SparkSession sparkSession, String sql){
        // Create view and execute query to display first 10 rows of WuzzufJobs data:
        return sparkSession.sql(sql);

    }

    public  void displaySummary(Dataset<Row> df){
        // Display Summary of Data:
        df.summary("count", "min", "max").show();
        long number_of_col = Arrays.stream(df.columns()).count();
        System.out.println("Number of column= "+number_of_col);
    }

    public void displayStructure(Dataset<Row> df){
        // Print Schema to see column names, types and other metadata
        df.printSchema();
    }

    public Dataset<Row> dropDuplicate(Dataset<Row> df){
        return df.dropDuplicates();
    }

    public Dataset<Row> dropNull(SparkSession sparkSession,Dataset<Row> df){

        String sql = "Select YearsExp from WuzzufJobs where YearsExp != \"null Yrs of Exp\"";

        return sparkSession.sql(sql);
    }
}
