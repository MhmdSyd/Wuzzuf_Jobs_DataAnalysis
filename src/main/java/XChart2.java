import org.knowm.xchart.*;
import java.awt.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;


public class XChart2 {

    public void createChart(Map testMap,String chartTitle,int element_num){
        // check that element is valid.
        if(element_num> testMap.size() || element_num < 0){
            element_num = testMap.size();
        }
        // Customize Chart by init dimensions and title for chart
        PieChart chart = new PieChartBuilder().width(800).height(600).title(chartTitle).build();
        // create list of color object that we need to display by different color.
        Color[] sliceColors = new Color[element_num];
        //use to generate list of  random color.
        for(int i=0;i<element_num;i++){
            sliceColors[i] = new Color((int) (Math.random( )*256), (int)(Math.random( )*256), (int)(Math.random( )*256));
        }
        // customize chart by take color list which will use at display.
        chart.getStyler().setSeriesColors(sliceColors);
        //create iterate from hash map to can loop on it.
        Iterator<Map.Entry<String, Integer>> itr = testMap.entrySet().iterator();

        //add element in chart to display.
        for(int i = 0;i<element_num;i++){
            Map.Entry<String, Integer> entry = itr.next();
            chart.addSeries(entry.getKey() ,entry.getValue());
        }
        //display chart.
        new SwingWrapper<PieChart>(chart).displayChart();

    }
}