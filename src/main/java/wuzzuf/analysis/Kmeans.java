package wuzzuf.analysis;

import javassist.expr.Cast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;


public  class Kmeans{
    public static String calculateKMeans() {
        // Loads data.
        SparkSession sparkSession = SparkSession.builder().appName ("Wuzzuf Jobs DataAnalysis").master("local[5]")
                .getOrCreate();
        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", true);
        Dataset<Row> dataset =  dataFrameReader.csv("src/main/resources/Wuzzuf_Jobs.csv");

// Trains a k-means model.

        dataset.createOrReplaceTempView ("WuzzufJobs");
        Dataset<Row> convert = sparkSession.sql("select Title, Company from WuzzufJobs");
        StringIndexerModel indexer = new StringIndexer()
                .setInputCols(new String[] { "Title", "Company"})
                .setOutputCols(new String[] { "TitleConv", "CompanyConv"})
                .fit(convert);
        Dataset<Row> indexed = indexer.transform(convert);
        indexed.show();
        indexed.printSchema();
        Dataset<Row> factorized = indexed.select("TitleConv", "CompanyConv");
        factorized.show();


        JavaRDD<Vector> parsedData = factorized.javaRDD().map(new Function<Row, Vector>() {
            public Vector call(Row s) {
                double[] values = new double[2];

                values[0] = Double.parseDouble(s.get(0).toString());
                values[1] = Double.parseDouble(s.get(1).toString());

                return Vectors.dense(values);}
        });

        parsedData.cache();
        int numClusters = 4;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
        double WithinError = clusters.computeCost(parsedData.rdd());

        String output = "<html><body>";
        output += "<h1>"+"Calculate KMeans Clustering:"+"</h1><br><p>";

        for (Vector str : clusters.clusterCenters()){
            output += str.toString();
            output += "<br>";
        }

        output += "<br>" + "Within Set Sum of Square Error: " + WithinError+"<br>";
        output += "</p></body></html>";
        return output;

    }


}