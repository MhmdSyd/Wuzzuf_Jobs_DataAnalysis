package wuzzuf.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.*;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PieCharts implements ExampleChart<PieChart> {
    // Method for creating pie chart of top ten companies posting jobs:
    public static <object> void popularCompanies(Dataset<Row> topTenComp) {
        ExampleChart<PieChart> popularCompChart = new PieCharts();
        // converting columns into lists for plotting pie chart
        List<Row> topCompanies = topTenComp.select("Company").collectAsList().stream().collect(Collectors.toList());
        List<Row> counts = topTenComp.select("JobCount").collectAsList().stream().collect(Collectors.toList());
        // calculate sum of job counts of top ten companies (used to calculate percentage of each company)
        int sumOfCounts = 0;
        for (Row x : counts){
            sumOfCounts += Integer.parseInt(x.get(0).toString());
        }

        List<String> top_companies = new ArrayList<>();
        for (Row x : topCompanies){
            top_companies.add(x.get(0).toString());
        }

        List<Double> Counts = new ArrayList<>();
        for (Row x : counts){
            Counts.add((Double.parseDouble(x.get(0).toString())/sumOfCounts)*100);
        }
        // passing lists to create pie chart and save the chart in chart PieChart object
        String xaxis = "";
        String title = "Top ten Companies Posting jobs at Wuzzuf";
        PieChart chart = popularCompChart.getChart(top_companies,Counts,title,xaxis);
        // Display the chart:
        //new SwingWrapper<PieChart>(chart).displayChart();
        try {
            BitmapEncoder.saveBitmap(chart,System.getProperty("user.dir")+"/Public/topCompanies", BitmapEncoder.BitmapFormat.PNG);
        }
        catch (Exception e){
            System.out.println(e);
            System.out.println("NOT Found path");
        }
    }


    @Override
    public PieChart getChart(List<String> keys, List<Double> values, String title, String xaxis) {

        // Create Chart
        PieChart chart = new PieChartBuilder().width(800).height(600).title(title).build();

        // Customize Chart
        Color[] sliceColors = new Color[] { new Color(224, 68, 14), new Color(227, 105, 62), new Color(230, 143, 110), new Color(233, 180, 159), new Color(236, 199, 182)
                ,new Color(239, 210, 14), new Color(242, 222, 62), new Color(245, 234, 110), new Color(248, 255, 159), new Color(251, 255, 182)};
        chart.getStyler().setSeriesColors(sliceColors);

        // Series
        for(int i = 0 ; i < keys.size() ; i++){
            chart.addSeries(keys.get(i), values.get(i));
        }
        return chart;
    }

}