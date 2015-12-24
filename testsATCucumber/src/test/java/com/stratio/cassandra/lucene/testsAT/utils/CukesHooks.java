package com.stratio.cassandra.lucene.testsAT.utils;
import gherkin.formatter.model.Background;
import gherkin.formatter.model.Examples;
import gherkin.formatter.model.Feature;
import gherkin.formatter.model.Match;
import gherkin.formatter.model.Result;
import gherkin.formatter.model.Scenario;
import gherkin.formatter.model.ScenarioOutline;
import gherkin.formatter.model.Step;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.cucumber.testng.ICucumberFormatter;
import com.stratio.cucumber.testng.ICucumberReporter;
import com.stratio.cassandra.lucene.testsAT.specs.BaseSpec;
import com.stratio.cassandra.lucene.testsAT.specs.Common;
public class CukesHooks extends BaseSpec implements ICucumberReporter, ICucumberFormatter{

    @Override
    public void before(Match match, Result result) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void result(Result result) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void after(Match match, Result result) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void match(Match match) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void embedding(String mimeType, byte[] data) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void write(String text) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void syntaxError(String state, String event, List<String> legalEvents, String uri, Integer line) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void uri(String uri) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void feature(Feature feature) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void scenarioOutline(ScenarioOutline scenarioOutline) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void examples(Examples examples) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void startOfScenarioLifeCycle(Scenario scenario) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void background(Background background) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void scenario(Scenario scenario) {
        // TODO Auto-generated method stub
        commonspec.getExceptions().clear();
    }

    @Override
    public void step(Step step) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void endOfScenarioLifeCycle(Scenario scenario) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void done() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void eof() {
        // TODO Auto-generated method stub
        
    }


}
