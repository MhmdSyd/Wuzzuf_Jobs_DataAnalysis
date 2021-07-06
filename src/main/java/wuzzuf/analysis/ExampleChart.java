package wuzzuf.analysis;

import org.knowm.xchart.internal.chartpart.Chart;

import java.util.List;

public interface ExampleChart<C extends Chart<?, ?>> {

  C getChart(List<String> keys, List<Double> values, String title, String yaxis);

  //String getExampleChartName();
}
