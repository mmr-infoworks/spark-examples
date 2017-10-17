package io.infoworks.spark.df;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.collection.Seq;
import static org.junit.Assert.assertEquals;


import java.util.List;


/**
 * Created by manoharm on 17/10/17.
 */
@RunWith(JUnit4.class)

public class BaseTest {
  final static String INPUTPATH = "df_integ_spark/examples/input/";
  final static String OUTPUTPATH = "df_integ_spark/examples/output/";


  public static SparkSession spark = SparkSession
                                       .builder().master("local")
                                       .appName("DF data sources example")
                                       .config("spark.some.config.option", "some-value")
                                       .getOrCreate();

  Dataset<Row> emp;
  Dataset<Row> dept;
  Dataset<Row> customer;
  Dataset<Row> store_returns;
  Dataset<Row> customer_address;
  Dataset<Row> store;
  Dataset<Row> store_sales;
  Dataset<Row> inventory;

  @Before
  public void setUp() throws Exception {

    emp = Source.getInstance().getSource(Format.JSON,input("emp.json"));
    dept = Source.getInstance().getSource(Format.JSON,input("dept.json"));
    customer =  Source.getInstance().getSource(Format.ORC,inputDfTpcDs("customer.orc"));
    store_returns =  Source.getInstance().getSource(Format.CSV,inputDfTpcDs("store_returns.csv"));
    customer_address =  Source.getInstance().getSource(Format.CSV,inputDfTpcDs("customer_address.csv"));
    store =  Source.getInstance().getSource(Format.ORC,inputDfTpcDs("store.orc"));
    store_sales =  Source.getInstance().getSource(Format.ORC,inputDfTpcDs("store_sales.orc"));
    inventory =  Source.getInstance().getSource(Format.CSV,inputDfTpcDs("inventory.csv"));
  }

  public static String input(String file) {
    return INPUTPATH +file;
  }
  public static String inputDfTpcDs(String file) {
    return INPUTPATH +"df_tpc_ds/" + file;
  }
  public static String output(String dir) {
    return OUTPUTPATH+dir;
  }

  public static Seq<Column> toCols(List<String> columns) {
    return ScalaUtils.toSeq(SparkUtils.toColumns(columns));
  }


}
