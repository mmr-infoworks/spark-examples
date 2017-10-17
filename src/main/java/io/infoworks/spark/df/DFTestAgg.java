package io.infoworks.spark.df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by manoharm on 17/10/17.
 */
public class DFTestAgg {
  final String INPUTPATH = "/Users/manoharm/infoworks/spark/examples/input";
  final String OUTPUTPATH = "/Users/manoharm/infoworks/spark/examples/output";
  public static SparkSession spark = SparkSession
                                       .builder().master("local")
                                       .appName("DF data sources example")
                                       .config("spark.some.config.option", "some-value")
                                       .getOrCreate();
  public static void main(String[] args) {
    Dataset<Row> emp = Source.getInstance().getSource(Format.JSON, "df_integ_spark/examples/input/emp.json");

    Dataset<Row> empFiltered = emp.filter("age > 20");
    System.out.println(empFiltered.collectAsList().size());
  }
}
