package com.example.springTrial.trial_one;



import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.util.List;

@RestController
public class RestConfig {

    @RequestMapping("/dataHead")
    public static String dataHead() throws FileNotFoundException {
        JobsAnalysis jobData = new JobsAnalysis();
        return jobData.displayHead();
    }

    @RequestMapping("/dataSummary")
    public static String dataSummary() throws FileNotFoundException {
        JobsAnalysis jobData = new JobsAnalysis();
        return jobData.dataSummary();
    }

    @RequestMapping("/dataStructure")
    public static String dataStructure() throws FileNotFoundException {
        JobsAnalysis jobData = new JobsAnalysis();
        return jobData.dataStructure();
    }

    @RequestMapping("/popularCompanies")
    public static String popularCompanies() throws FileNotFoundException {
        JobsAnalysis jobData = new JobsAnalysis();
        return jobData.jobsCountPerComapny();
    }

    @RequestMapping("/popularTitles")
    public static String popularTitles() throws FileNotFoundException {
        JobsAnalysis jobData = new JobsAnalysis();
        return jobData.topTitles();
    }

    @RequestMapping("/popularAreas")
    public static String popularAreas() throws FileNotFoundException {
        JobsAnalysis jobData = new JobsAnalysis();
        return jobData.topAreas();
    }

    @RequestMapping("/popularSkills")
    public static String popularSkills() throws FileNotFoundException {
        JobsAnalysis jobData = new JobsAnalysis();
        return jobData.topJobSkills();
    }

    @RequestMapping("/factorize")
    public static String factorize() throws FileNotFoundException {
        JobsAnalysis jobData = new JobsAnalysis();
        return jobData.factorizeAndConvert();
    }

    @RequestMapping("/getKMeans")
    public String getKMeans(){
        return Kmeans.calculateKMeans();
    }



    @RequestMapping("/dashBoard")
    public String sayBye() throws Exception
    {

        BufferedReader reader = new BufferedReader(new FileReader ("src/main/resources/Images.html"));
        String line = null;
        StringBuilder  stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");

        try {
            while((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }

            return stringBuilder.toString();
        } finally {
            reader.close();
        }

    }



}
