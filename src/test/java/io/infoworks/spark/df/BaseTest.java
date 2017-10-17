package io.infoworks.spark.df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;



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

  @Before
  public void setUp() throws Exception {

      emp = Source.getInstance().getSource(Format.JSON,input("emp.json"));
      dept = Source.getInstance().getSource(Format.JSON,input("dept.json"));
  }

  public static String input(String file) {
    return INPUTPATH +file;
  }
  public static String output(String dir) {
    return OUTPUTPATH+dir;
  }


}
