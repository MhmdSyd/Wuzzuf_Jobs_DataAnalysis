package wuzzuf.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.stream.Collectors;

public class JobsAnalysis {
    private WuzzufJobsDAO jobData;

    public JobsAnalysis(){
        // create object and give path of data
        this.jobData = new WuzzufJobsDAO("src/main/resources/Wuzzuf_Jobs.csv");
    }

    public  String displayHead(){
        Dataset<Row> head = jobData.getHeadData(10);
        return getOutput(head, "Data Head:");
    }

    public String dataSummary(){
        Dataset<Row> summary = jobData.getSummary();
        return getOutput(summary, "Data Summary:");
    }

    public  String dataStructure(){
        String structure = jobData.getStructure();
        String str = structure.replace("StructType", "").replace("StructField","").replace("(","").replace(")","");
        String output = "<html><body>";
        output += "<h1>Job Data Structure:</h1><br><p>";
        String[] output_list = str.split(",");
        String[] x = {"Column Name", "Column Type", "Nullable"};
        for(int i = 0 ; i < output_list.length ;){
            int j = i;
            for( j = i ; j < i+3 ; j++){
                output += x[j%3] + " -->" + output_list[j];
                output += "<br>";
            }
            i = j;
            output += "<br>";
        }
        output += "</p></body></html>";
        return output;
    }

    // Most Demanding companies for jobs:
    public String jobsCountPerComapny(){
        String query = "SELECT Company, COUNT(Title) AS JobCount " +
                "FROM companyFreq " +
                "GROUP BY Company " +
                "HAVING Company <> 'Confidential' "+
                "ORDER BY JobCount DESC "+
                "LIMIT 10";
        // Get the required data:
        Dataset<Row> jobsPerCmp = jobData.sqlQuery(query, "companyFreq");
        // Visualize top ten companies in pie chart:
        PieCharts.popularCompanies(jobsPerCmp);

        return getOutput(jobsPerCmp, "Top Ten Popular Companies:");
    }

    // Most Popular job Titles:
    public String topTitles() {
        String query = "SELECT Title, COUNT(*) AS TitlesCount " +
                "FROM titleFreq " +
                "GROUP BY Title " +
                "ORDER BY TitlesCount DESC " +
                "LIMIT 10";
        // Get the required data:
        Dataset<Row> top_titles = jobData.sqlQuery(query, "titleFreq");
        // Visualize top ten popular Titles in bar chart:
        BarCharts.popularTitles(top_titles);

        return getOutput(top_titles, "Top Ten Popular Titles");
    }

    // Most Popular job Areas:
    public String topAreas(){
        String query = "SELECT Location, COUNT(*) AS Area_Frequency " +
                "FROM areaFreq " +
                "GROUP BY Location " +
                "ORDER BY Area_Frequency DESC " +
                "LIMIT 10";
        // Get the required data:
        Dataset<Row> top_areas = jobData.sqlQuery(query, "areaFreq");
        // Visualize top ten popular Areas in bar chart:
        BarCharts.popularAreas(top_areas);

        return getOutput(top_areas, "Top Ten Popular Areas:");
    }

    // Most Popular Skills:
    public String topJobSkills(){
        String sqlQuery = "SELECT Skills From topSkills";
        Dataset<Row> Skills = jobData.sqlQuery(sqlQuery, "topSkills");

        // convert column into list
        List<Row> skills = Skills.collectAsList().stream().collect(Collectors.toList());
        // create hashmap to contain how many each skill has repeated:
        HashMap<String, Integer> skillsFreq = new HashMap<>();
        for (Row row : skills) {
            String[] rowStr = row.toString().split(",");
            for (String x : rowStr) {
                x = x.trim().replace("[", "").replace("]", "");
                if (skillsFreq.containsKey(x)) {
                    skillsFreq.put(x, skillsFreq.get(x) + 1);
                } else {
                    skillsFreq.put(x, 1);
                }
            }
        }
        // sort skills by its counts:
        skillsFreq = sortByValue(skillsFreq);

        // Take top ten Skills
        LinkedHashMap<String,Integer> topTenSkills = new LinkedHashMap<>();
        // create iterator to iterate over the linkedHashMap
        Iterator<Map.Entry<String, Integer>> skillFreqIterator = skillsFreq.entrySet().iterator();
        for (int i = 0; i<10 ; i++) {
            Map.Entry<String, Integer> entry = skillFreqIterator.next();
            topTenSkills.put(entry.getKey() , entry.getValue());
        }
        BarCharts.topSkills(topTenSkills);

        //output:
        String output = "<html><body>";
        output += "<h1>Top Ten Popular Skills</h1><br><p>";
        for(String k : topTenSkills.keySet()){
                output = output + "Skill --> " +k + "<br> Counts -->" + topTenSkills.get(k) + "<br><br>";
        }
        output += "</p></body></html>";

        return output;
    }

    // Sorting HashMap by values:
    private LinkedHashMap<String, Integer> sortByValue(HashMap<String, Integer> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list =
                new LinkedList<Map.Entry<String, Integer> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2)
            {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // put data from sorted list to hashmap
        LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    public String factorizeAndConvert(){
        Dataset<Row> factorize = jobData.factorizeAndConvert();
        return getOutput(factorize, "Top Ten Popular Titles");
    }


    private String getOutput(Dataset<Row> ds, String header){
        // Output
        List<String> data = ds.toJSON().collectAsList();

        String output = "<html><body>";
        output += "<h1>"+header+"</h1><br><p>";
        for (String str : data){
            String s = str.toString().replace("\"", "").replace(":", "-->");
            String[] strList = s.split(",",8);
            for (String attrib : strList){
                output = output + attrib;
                output += "<br>";
            }
            output += "<br>";
        }
        output += "</p></body></html>";
        return output;
    }
}
