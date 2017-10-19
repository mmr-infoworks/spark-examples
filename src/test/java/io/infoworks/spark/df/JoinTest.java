package io.infoworks.spark.df;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

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




  private Dataset<Row> getRenamedDf(Dataset<Row> df,String prefix) {
    String[] columns = df.columns();
    Dataset<Row> dfRenamed = df;
    for (String col:columns) {
      dfRenamed = dfRenamed.withColumnRenamed(col,prefix+"_"+col);
    }
    return dfRenamed;
  }
}
