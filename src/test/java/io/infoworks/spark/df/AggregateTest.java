package io.infoworks.spark.df;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.spark.sql.functions.*;

import io.infoworks.spark.df.ScalaUtils;
import io.infoworks.spark.df.SparkUtils;

/**
 * Created by manoharm on 17/10/17.
 */
public class AggregateTest extends BaseTest{

  @Test
  public void aggColumn1() {

    /***
     select count(* ), c_salutation from customer group by c_salutation ;

     | _c0  | c_salutation  |
     +------+---------------+--+
     | 13   | Dr.           |
     | 2    | Miss          |
     | 10   | Mr.           |
     | 8    | Mrs.          |
     | 5    | Ms.           |
     | 12   | Sir           |
     +------+---------------+--+
     6 rows selected (4.09 seconds)
     */
    customer.groupBy("c_salutation").count().printSchema();
    Dataset<Row> cnt = customer.groupBy("c_salutation").count();
    assert(customer.groupBy("c_salutation").count().collectAsList().size() == 6);
  }
  @Test
  public void aggColumn2() {
    /***
     select count(*) , c_salutation , c_preferred_cust_flag from customer
     group by c_salutation , c_preferred_cust_flag;

     +------+---------------+------------------------+--+
     | _c0  | c_salutation  | c_preferred_cust_flag  |
     +------+---------------+------------------------+--+
     | 6    | Dr.           | N                      |
     | 7    | Dr.           | Y                      |
     | 1    | Miss          | N                      |
     | 1    | Miss          | Y                      |
     | 6    | Mr.           | N                      |
     | 4    | Mr.           | Y                      |
     | 6    | Mrs.          | N                      |
     | 2    | Mrs.          | Y                      |
     | 3    | Ms.           | N                      |
     | 2    | Ms.           | Y                      |
     | 5    | Sir           | N                      |
     | 7    | Sir           | Y                      |
     +------+---------------+------------------------+--+
     12 rows selected (8.931 seconds)
     */
    List<String> columns = Arrays.asList("c_salutation","c_preferred_cust_flag");


    assert(customer.groupBy(ScalaUtils.toSeq(SparkUtils.toColumns(columns))).count().collectAsList().size() == 12);
  }

  @Test
  public void aggColumn2OMaxSumetc() {
    /***
     * select count(*) ,ss_customer_sk,ss_store_sk, sum(ss_sales_price) , max(ss_ext_discount_amt) from store_sales group by ss_customer_sk,ss_store_sk

     +------+-----------------+--------------+---------+----------+--+
     | _c0  | ss_customer_sk  | ss_store_sk  |   _c3   |   _c4    |
     +------+-----------------+--------------+---------+----------+--+
     | 2    | NULL            | NULL         | 35.43   | 0        |
     | 1    | NULL            | 8            | NULL    | 0        |
     | 11   | 11683           | 8            | 427.02  | 1397.16  |
     | 1    | 35459           | 2            | 86.17   | 0        |
     | 10   | 39335           | 10           | 477.69  | 583.12   |
     | 14   | 40891           | 8            | 579.19  | 1033.03  |
     | 11   | 60434           | 1            | 629.23  | 3132.19  |
     7 rows selected (8.581 seconds)
     */

    /*

    +--------------+-----------+------------------------+-------------------+
|ss_customer_sk|ss_store_sk|max(ss_ext_discount_amt)|sum(ss_sales_price)|
+--------------+-----------+------------------------+-------------------+
|         40891|          8|                 1033.03|             579.19|
|         35459|          2|                    0.00|              86.17|
|          null|       null|                    0.00|              35.43|
|         39335|         10|                  583.12|             477.69|
|         11683|          8|                 1397.16|             427.02|
|          null|          8|                    0.00|               null|
|         60434|          1|                 3132.19|             629.23|
+--------------+-----------+------------------------+-------------------+
     */

    //key is ss_customer_sk$$ss_store_sk value is value of max(ss_ext_discount_amt)
    Map<String,String> expectedResult = new HashMap<>();
    expectedResult.put("40891$$8","1033.03");
    expectedResult.put("35459$$2","0.00");
    expectedResult.put("null$$null","0.00");
    expectedResult.put("39335$$10","583.12");
    expectedResult.put("11683$$8","1397.16");
    expectedResult.put("null$$8","0.00");
    expectedResult.put("60434$$1","3132.19");
    List<String> columns = Arrays.asList("ss_customer_sk","ss_store_sk");
    Map<String,String> aggregatedColumnMap = new HashMap<>();

    aggregatedColumnMap.put("ss_sales_price","sum");
    aggregatedColumnMap.put("ss_ext_discount_amt","max");

    //store_sales.registerTempTable("store_sales");

    //Expression expression = spark.sessionState().sqlParser().parseExpression("sum(ss_sales_price + 1)/ max(ss_ext_discount_amt)");
    //Column col = new Column(expression);
    //Column col = new Column(expression);
    //Dataset<Row> sqlAggrDF  = spark.sql("select count(*) ,ss_customer_sk,ss_store_sk, sum(ss_sales_price + 1) / max(ss_ext_discount_amt) from store_sales group by ss_customer_sk,ss_store_sk");

    //sqlAggrDF.explain(true);

    List<Row> rows = store_sales.groupBy(toCols(columns)).agg(aggregatedColumnMap).collectAsList();
    Map<String,String> resultMap  = ResultCheckHelper.makeMap(rows,Arrays.asList(0,1),Arrays.asList(2));
    assert(ResultCheckHelper.compareMap(expectedResult,resultMap));
    System.out.println();
    //store_sales.groupBy(ScalaUtils.toSeq(SparkUtils.toColumns(columns))).agg(aggregatedColumnMap).show();
  }

  @Test
  public void aggrUsingComplexExpression() {
    /**
     *  select count(*) ,ss_customer_sk + 1 ,ss_store_sk/2, sum(ss_sales_price) / max(ss_ext_discount_amt)
     from store_sales group by ss_customer_sk + 1,ss_store_sk/2;

     +------+--------+-------+---------------+--+
     | _c0  |  _c1   |  _c2  |      _c3      |
     +------+--------+-------+---------------+--+
     | 2    | NULL   | NULL  | NULL          |
     | 1    | NULL   | 4     | NULL          |
     | 11   | 11684  | 4     | 0.3056342867  |
     | 1    | 35460  | 1     | NULL          |
     | 10   | 39336  | 5     | 0.8191967348  |
     | 14   | 40892  | 4     | 0.5606710357  |
     | 11   | 60435  | 0.5   | 0.2008913891  |
     +------+--------+-------+---------------+--+
     7 rows selected (10.047 seconds)
     */

    Expression aggColExpr = spark.sessionState().sqlParser().parseExpression("sum(ss_sales_price )/ max(ss_ext_discount_amt)");
    Expression groupByColExpr1 = spark.sessionState().sqlParser().parseExpression("ss_customer_sk + 1");
    Expression groupByColExpr2 = spark.sessionState().sqlParser().parseExpression("ss_store_sk/2");
    Column aggCol = new Column(aggColExpr);
    Column groupByCol1 = new Column(groupByColExpr1);
    Column groupByCol2 = new Column(groupByColExpr2);
    Dataset<Row> df = store_sales.groupBy(groupByCol1,groupByCol2).agg(aggCol);
    df.show();

    List<Row> rows = df.collectAsList();


  }
}
