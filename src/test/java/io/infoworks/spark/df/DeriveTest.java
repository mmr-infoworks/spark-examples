package io.infoworks.spark.df;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by manoharm on 18/10/17.
 */
public class DeriveTest extends BaseTest {

  @Test
  public void derive1() throws Exception {
    /***
     * select c_customer_sk, c_customer_sk + 1 from customer;
     +----------------+---------+--+
     | c_customer_sk  |   _c1   |
     +----------------+---------+--+
     | 100000         | 100001  |
     | 99999          | 100000  |
     .. etc
     | 99953          | 99954   |
     | 99952          | 99953   |
     | 99951          | 99952   |
       50 rows
     */
    Map<String,String> expectedMap  = new HashMap<>();
    for (int i = 0; i < 50; i++) {
       expectedMap.put(String.valueOf(99951+i),String.valueOf(99952+i));
    }

    Expression deriveExpr =  spark.sessionState().sqlParser().parseExpression("c_customer_sk + 1");
    Column deriveColumn = new Column(deriveExpr);
    Column normal = new Column("c_customer_sk");
    List<Row> resultList = customer.select(normal,deriveColumn).collectAsList();
    Map<String,String> resultMap = ResultCheckHelper.makeMap(resultList, Arrays.asList(0),Arrays.asList(1));
    assert (ResultCheckHelper.compareMap(expectedMap,resultMap));
  }
}
