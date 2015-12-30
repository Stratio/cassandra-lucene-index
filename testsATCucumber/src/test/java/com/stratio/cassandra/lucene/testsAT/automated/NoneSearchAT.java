package com.stratio.cassandra.lucene.testsAT.automated;

import com.stratio.cassandra.lucene.testsAT.utils.BaseTest;
import com.stratio.cucumber.testng.CucumberRunner;
import cucumber.api.CucumberOptions;
import org.testng.annotations.Test;

@CucumberOptions(features = {"src/test/resources/features/noneSearch.feature"})
public class NoneSearchAT extends BaseTest {
    public NoneSearchAT() {
    }

    @Test(enabled = true, priority = 1)
    public void g() throws Exception {
        new CucumberRunner(this.getClass()).runCukes();
    }
}
