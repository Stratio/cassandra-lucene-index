package com.stratio.cassandra.lucene.testsAT.automated;

import com.stratio.cassandra.lucene.testsAT.utils.BaseTest;
import com.stratio.cucumber.testng.CucumberRunner;
import cucumber.api.CucumberOptions;
import org.testng.annotations.Test;

@CucumberOptions(features = {"src/test/resources/features/boolean.feature"})
public class BooleanAT extends BaseTest {
    public BooleanAT() {
    }

    @Test(enabled = true, priority = 1)
    public void g() throws Exception {
        new CucumberRunner(this.getClass()).runCukes();
    }
}
