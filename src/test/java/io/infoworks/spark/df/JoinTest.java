package io.infoworks.spark.df;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import org.stringtemplate.v4.ST;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.infoworks.spark.df.SparkUtils.getColumnFromExp;

/**
 * Created by manoharm on 17/10/17.
 */
public class JoinTest extends BaseTest {
  Dataset<Row> storeRenamed;
  Dataset<Row> storesalesRenamed;

  @Before
  public void renameSources() throws Exception {
     storeRenamed = getRenamedDf(store,"store");
     storesalesRenamed = getRenamedDf(store_sales,"store_sales");
  }

  @Test
  public void innerJoin1() {
      storeRenamed.printSchema();
      storesalesRenamed.printSchema();

    /***
     * select count(*) from store s join store_sales ss on s.s_store_sk = ss.ss_store_sk;
     +------+--+
     | _c0  |
     +------+--+
     | 48   |
     +------+--+
     */
    Column joinCond = getColumnFromExp(spark,"store_s_store_sk = store_sales_ss_store_sk");
    Dataset<Row> joined = storeRenamed.join(storesalesRenamed,joinCond,"inner");
    List<Row> joinedList = joined.collectAsList();
    assert (joinedList.size() == 48);

  }

  @Test
  public void innerJoin2() {
    storeRenamed.printSchema();
    storesalesRenamed.printSchema();

    /***
     * select count(*) from store s join store_sales ss on s.s_store_sk = ss.ss_store_sk and s.s_store_sk > 7;

     +------+--+
     | _c0  |
     +------+--+
     | 36   |
     +------+--+
     1 row selected (9.237 seconds)
     */
    Column joinCond = getColumnFromExp(spark,"store_s_store_sk = store_sales_ss_store_sk and store_s_store_sk > 7");
    Dataset<Row> joined = storeRenamed.join(storesalesRenamed,joinCond,"inner");
    List<Row> joinedList = joined.collectAsList();
    assert (joinedList.size() == 36);

  }

  @Test
  public void leftJoin1() {
    storeRenamed.printSchema();
    storesalesRenamed.printSchema();

    /***
     * select count(*) from store s left join store_sales ss on s.s_store_sk = ss.ss_store_sk
     and ss.ss_store_sk =1000;

     +------+--+
     | _c0  |
     +------+--+
     | 12   |
     +------+--+
     */
    Column joinCond = getColumnFromExp(spark,"store_s_store_sk = store_sales_ss_store_sk and store_sales_ss_store_sk = 1000");
    Dataset<Row> joined = storeRenamed.join(storesalesRenamed,joinCond,"left");
    List<Row> joinedList = joined.collectAsList();
    assert (joinedList.size() == 12);

  }

  @Test
  public void leftJoin2() {
    storeRenamed.printSchema();
    storesalesRenamed.printSchema();

    /***
     * select count(*) from store s left join store_sales ss on s.s_store_sk = ss.ss_store_sk
     and ss.ss_store_sk =1000;

     +------+--+
     | _c0  |
     +------+--+
     | 12   |
     +------+--+
     */
    Column joinCond = getColumnFromExp(spark,"store_s_store_sk = store_sales_ss_store_sk and store_sales_ss_store_sk = 1000");
    Dataset<Row> joined = storeRenamed.join(storesalesRenamed,joinCond,"left");
    List<Row> joinedList = joined.collectAsList();
    assert (joinedList.size() == 12);

  }

  @Test
  public void rightJoin1() {
    storeRenamed.printSchema();
    storesalesRenamed.printSchema();

    /***
     select ss.ss_customer_sk, (ss.ss_item_sk * ss.ss_customer_sk )/10 from store s
     right join  store_sales ss on s.s_store_sk = ss.ss_store_sk
     and ss.ss_store_sk > 1000 and trim(s.ziw_status_flag) = 'I';

     +--------------------+--------------+--+
     | ss.ss_customer_sk  |     _c1      |
     +--------------------+--------------+--+
     | 39335              | 37895339     |      | 39335              | 57023949.5   |      | 39335              | 39063588.5   |      | 39335              | 40113833     |
     | 39335              | 48271912     |      | 40891              | 48263647.3   |      | 40891              | 46043266     |      | 40891              | 72467030.2   |
     | 40891              | 40694723.2   |      | NULL               | NULL         |      | 40891              | 71992694.6   |      | 40891              | 62444646.1   |
     | 40891              | 62252458.4   |      | 11683              | 19078339     |      | 11683              | 20490813.7   |      | 11683              | 12443563.3   |
     | 60434              | 100338570.2  |      | 60434              | 89085759.4   |      | 60434              | 86342055.8   |      | 60434              | 65443978.6   |
     | 60434              | 107264306.6  |      | 39335              | 22358014     |      | 39335              | 35145822.5   |      | 39335              | 8622232      |
     | 39335              | 12831077     |      | 39335              | 27711507.5   |      | 40891              | 6334015.9    |      | 40891              | 12860219.5   |
     | 40891              | 30013994     |      | 40891              | 10165502.6   |      | 40891              | 21623160.8   |      | 40891              | 28182077.2   |
     | 40891              | 4199505.7    |      | 11683              | 10119814.6   |      | 11683              | 5703640.6    |      | 11683              | 3944180.8    |
     | 11683              | 1278120.2    |      | 11683              | 10624520.2   |      | 11683              | 7911727.6    |      | 11683              | 10918931.8   |
     | NULL               | NULL         |      | 11683              | 3660283.9    |      | 60434              | 19913003     |      | 60434              | 15452973.8   |
     | NULL               | NULL         |      | 60434              | 54432903.8   |      | 60434              | 7475685.8    |      | 60434              | 33836996.6   |
     | 60434              | 24433466.2   |      | 35459              | 23108630.3
     50 rows selected (3.96 seconds)
     */

    Map<String,String> expectedMap = new HashMap<>();

    List<String> keys = Arrays.asList("39335$$37895339", "39335$$57023949.5", "39335$$39063588.5", "39335$$40113833", "39335$$48271912", "40891$$48263647.3", "40891$$46043266", "40891$$72467030.2", "40891$$40694723.2", "null", "40891$$71992694.6", "40891$$62444646.1", "40891$$62252458.4", "11683$$19078339", "11683$$20490813.7", "11683$$12443563.3", "60434$$100338570.2", "60434$$89085759.4", "60434$$86342055.8", "60434$$65443978.6", "60434$$107264306.6", "39335$$22358014", "39335$$35145822.5", "39335$$8622232", "39335$$12831077", "39335$$27711507.5", "40891$$6334015.9", "40891$$12860219.5", "40891$$30013994", "40891$$10165502.6", "40891$$21623160.8", "40891$$28182077.2", "40891$$4199505.7", "11683$$10119814.6", "11683$$5703640.6", "11683$$3944180.8", "11683$$1278120.2", "11683$$10624520.2", "11683$$7911727.6", "11683$$10918931.8", "null", "11683$$3660283.9", "60434$$19913003", "60434$$15452973.8", "null", "60434$$54432903.8", "60434$$7475685.8", "60434$$33836996.6", "60434$$24433466.2", "35459$$23108630.3");
    for (String key : keys) {
      expectedMap.put(key,key);
    }
     Column joinCond = getColumnFromExp(spark,
      "store_s_store_sk = store_sales_ss_store_sk and store_sales_ss_store_sk > 1000 " +
        "and trim(store_ziw_status_flag) = 'I'");
    Dataset<Row> joined = storeRenamed.join(storesalesRenamed,joinCond,"right");
    Column col1 = getColumnFromExp(spark,"store_sales_ss_customer_sk");
    Column col2 = getColumnFromExp(spark,"(store_sales_ss_item_sk * store_sales_ss_customer_sk )/10");
    Dataset<Row> selected = joined.select(col1,col2);
    List<Row> selectedList = selected.collectAsList();

    Map<String,String> resultMap  = ResultCheckHelper.makeMap(selectedList, Arrays.asList(0,1),Arrays.asList(0,1));
     assert (selectedList.size()==50);
    //TODO fix the float check
    // assert (ResultCheckHelper.compareMap(expectedMap,resultMap));

  }


  private Dataset<Row> getRenamedDf(Dataset<Row> df,String prefix) {
    String[] columns = df.columns();
    Dataset<Row> dfRenamed = df;
    for (String col:columns) {
      dfRenamed = dfRenamed.withColumnRenamed(col,prefix+"_"+col);
    }
    return dfRenamed;
  }
}
