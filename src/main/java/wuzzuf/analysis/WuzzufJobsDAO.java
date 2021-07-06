package wuzzuf.analysis;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WuzzufJobsDAO {

    private Dataset<Row> data = null;
    private static Dataset<Row> dataUnclean = null;
    private String path = null;
    private SparkSession sparkSession = null;

    public WuzzufJobsDAO(String path){
        this.path = path;
        this.readCsvData();
        this.readCsvCleanData();
    }

    //edit Csv File path.
    public void setPath(String path){
        this.path = path;

    }

    // get datasets of Csv File any where any time.
    public Dataset<Row> getData(){
        return this.data;
    }

    // return dataset of Csv File without Cleaning.
    public void readCsvData(){
        this.sparkSession = SparkSession.builder().appName ("Wuzzuf Jobs DataAnalysis").master("local[5]")
                .getOrCreate();
        // Get DataFrameReader using SparkSession.
        DataFrameReader dataFrameReader = this.sparkSession.read();

        // Set header option to true to specify that first row in file contains column name.
        dataFrameReader.option("header", true);


        this.dataUnclean =  dataFrameReader.csv("src/main/resources/Wuzzuf_Jobs.csv");
    }

    // return dataset of Csv File After Cleaning data and drop duplication.
    public void readCsvCleanData(){
        this.data = this.dataUnclean.dropDuplicates();
        this.data.createOrReplaceTempView ("WuzzufJobs");
        String sql = "Select * from WuzzufJobs where YearsExp != \"null Yrs of Exp\"";
        this.data = sparkSession.sql(sql);
    }

    // return some of first rows from data ---- it need number of line that you want to display.
    public Dataset<Row> getHeadData(int number){
        // Create view and execute query to display first n rows of WuzzufJobs data:
        this.dataUnclean.createOrReplaceTempView ("WuzzufJobs");
        return this.sparkSession.sql("SELECT * FROM WuzzufJobs LIMIT "+ number+";");
    }

    // return dataset of special data which you need from sqlQuery.
    public Dataset<Row> sqlQuery(String sql,String viewName){
        // Create view and execute query what  you want.
        this.data.createOrReplaceTempView (viewName);
        return this.sparkSession.sql(sql);

    }
    public  Dataset<Row> getSummary(){
        // Display Summary of Data:
        return this.dataUnclean.summary("count");

    }

    public String getStructure(){
        // Print Schema to see column names, types and other metadata
        String r = this.dataUnclean.schema().toString();

        return r;

    }

    public Dataset<Row> factorizeAndConvert(){
        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("YearsExp")
                .setOutputCol("YearsExp_Conv")
                .fit(this.data);
        return indexer.transform(this.data).limit(10);
    }

}
