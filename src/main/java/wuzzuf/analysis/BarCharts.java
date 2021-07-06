package wuzzuf.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.style.Styler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class BarCharts implements ExampleChart<CategoryChart> {

    // Method for creating bar chart of top ten Titles in dataset:
    public static void popularTitles(Dataset<Row> topTenTitles){
        ExampleChart<CategoryChart> exampleChart = new BarCharts();

        // converting columns into lists for plotting pie chart
        List<Row> top_titles = topTenTitles.select("Title").collectAsList().stream().collect(Collectors.toList());
        List<Row> counts = topTenTitles.select("TitlesCount").collectAsList().stream().collect(Collectors.toList());

        List<String> topTitles = new ArrayList<>();
        for (Row x : top_titles){
            topTitles.add(x.get(0).toString());
        }

        List<Double> Counts = new ArrayList<>();
        for (Row x : counts){
            Counts.add(Double.parseDouble(x.get(0).toString()));
        }
        String xaxis = "Titles";
        String title = "Top Ten Titles";
        CategoryChart chart = exampleChart.getChart(topTitles, Counts, title, xaxis);
        //new SwingWrapper<>(chart).displayChart();
        try {
            BitmapEncoder.saveBitmap(chart, System.getProperty("user.dir")+"/Public/topTitles", BitmapEncoder.BitmapFormat.PNG);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }


    // Method for creating bar chart of top ten Titles in dataset:
    public static void popularAreas(Dataset<Row> topTenAreas) {
        ExampleChart<CategoryChart> exampleChart = new BarCharts();

        // converting columns into lists for plotting pie chart
        List<Row> top_Areas = topTenAreas.select("Location").collectAsList().stream().collect(Collectors.toList());
        List<Row> counts = topTenAreas.select("Area_Frequency").collectAsList().stream().collect(Collectors.toList());

        List<String> topAreas = new ArrayList<>();
        for (Row x : top_Areas){
            topAreas.add(x.get(0).toString());
        }

        List<Double> Counts = new ArrayList<>();
        for (Row x : counts){
            Counts.add(Double.parseDouble(x.get(0).toString()));
        }
        String xaxis = "Areas";
        String title = "Top Ten Areas";
        CategoryChart chart = exampleChart.getChart(topAreas, Counts, title, xaxis);
        //new SwingWrapper<>(chart).displayChart();
        try {
            BitmapEncoder.saveBitmap(chart, System.getProperty("user.dir")+"/Public/topAreas", BitmapEncoder.BitmapFormat.PNG);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    //Method for Draw top ten skills in a bar chart:
    public static void topSkills(LinkedHashMap<String,Integer> topTenSkills){
        ExampleChart<CategoryChart> exampleChart = new BarCharts();

        List<String> skills = new ArrayList<>();
        List<Double> Counts = new ArrayList<>();
        for(String k : topTenSkills.keySet()){
            skills.add(k);
            Counts.add(Double.valueOf(topTenSkills.get(k)));
        }

        String xaxis = "Skills";
        String title = "Top Ten Skills";
        CategoryChart chart = exampleChart.getChart(skills, Counts, title, xaxis);
        //new SwingWrapper<>(chart).displayChart();
        try {
            BitmapEncoder.saveBitmap(chart, System.getProperty("user.dir")+"/Public/topSkills", BitmapEncoder.BitmapFormat.PNG);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    @Override
    public CategoryChart getChart(List<String> keys, List<Double> values, String title, String xaxis) {

        // Create Chart
        CategoryChart chart =
                new CategoryChartBuilder()
                        .width(1800)
                        .height(800)
                        .title(title)
                        .xAxisTitle(xaxis)
                        .yAxisTitle("Counts")
                        .build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        //chart.getStyler().setLabelsVisible(false);
        chart.getStyler().setPlotGridLinesVisible(false);

        // Series
        chart.addSeries(xaxis, keys, values);

        return chart;
    }

}
