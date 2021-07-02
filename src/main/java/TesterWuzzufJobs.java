import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;

import java.lang.reflect.MalformedParameterizedTypeException;
import java.util.HashMap;
import java.util.Map;

public class TesterWuzzufJobs {
    final static String path = "src/main/resources/Wuzzuf_Jobs.csv";
    public static void main(String[] args) throws Exception{
        //create object from wuzzufJobs class to can run method.
        WuzzufJobsDAO wuzzuf = new WuzzufJobsDAO();
        // open Spark Session.
        SparkSession sparkSession = wuzzuf.openSession("Wuzzuf Jobs DataAnalysis",5);
        // read Csv File and return values as Dataset of Rows.
        Dataset<Row> wuzzufDF = wuzzuf.readCsvData(sparkSession,path,true);
        //1- Display some of data (first some rows of data).
        wuzzuf.displayHead(sparkSession,wuzzufDF,5);
        //2- Display summary of data
        wuzzuf.displaySummary(wuzzufDF);
        //2- Display Structure
        wuzzuf.displayStructure(wuzzufDF);
        //3- remove duplication
        wuzzufDF = wuzzuf.dropDuplicate(wuzzufDF);
        //3- clean null
        wuzzufDF = wuzzuf.dropNull(sparkSession, wuzzufDF);

        //4- count jobs for each company.
        String sqlLine = "SELECT Company,COUNT(DISTINCT(Title)) as jobCount FROM WuzzufJobs "+
                "Group by Company Order by COUNT(DISTINCT(Title)) DESC LIMIT 10;";
        Dataset<Row> companyJobs = wuzzuf.sqlQuery(sparkSession,sqlLine);
        companyJobs.show();

        //5- display result from step 4 pie chart
        //create Map to add it to XChart
        Map<String,Integer> mapList = new HashMap<>();
        // convert Dataset to Map<key,value>
        companyJobs.collectAsList().stream()
                .forEach(i->mapList.put(i.get(0).toString().trim(), Integer.valueOf(i.get(1).toString().trim())));
        // display company with jobs by XChart
        XChart2 chat = new XChart2();
        chat.createChart(mapList,"Company with number of Jobs",10);

        //6- what are the most popular job Title?
        sqlLine ="SELECT Title, COUNT(Title) as myCount FROM WuzzufJobs GROUP BY Title Order by myCount DESC LIMIT 10;";
        Dataset<Row> popularTitle = wuzzuf.sqlQuery(sparkSession,sqlLine);
        popularTitle.show();

        // 7- bar chart step 6

        //8- most popular Areas.
        sqlLine ="SELECT Location, COUNT(Location) as myCount FROM WuzzufJobs GROUP BY Location Order by myCount DESC LIMIT 10;";
        Dataset<Row> popularAreas = wuzzuf.sqlQuery(sparkSession,sqlLine);
        popularAreas.show();

        //9-

        //10-


    }
}
