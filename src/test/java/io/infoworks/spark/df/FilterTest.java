package io.infoworks.spark.df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
/**
 * Created by manoharm on 17/10/17.
 */
@RunWith(JUnit4.class)

public class FilterTest extends BaseTest {

  @Test
  public void filter1() throws Exception {
    Dataset<Row> empFiltered = emp.filter("age > 20");
    assertEquals(empFiltered.collectAsList().size(), 2);
    assertEquals(emp.collectAsList().size(),3);
  }
}
