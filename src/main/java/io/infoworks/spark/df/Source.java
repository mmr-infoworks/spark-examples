package io.infoworks.spark.df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by manoharm on 16/10/17.
 */
public class Source {
  SparkSession spark = DFSparkContext.spark;

  private static  Source INSTANCE = new Source();
  private  Source() {

  }
  public static Source getInstance(){
    return INSTANCE;
  }
  public Dataset<Row> getSource(Format format, String path) { // TODO change string path to sourceproerties

    Dataset<Row> dataset;
    switch (format) {
      case JSON:
          dataset = spark.read().json(path);
          break;
      case CSV:
          dataset = spark.read().option("header", true).option("inferSchema", true).csv(path);
          break;
      case ORC:
          dataset = spark.read().option("header", true).option("inferSchema", true).orc(path);
          break;
      default:
        throw new RuntimeException(" format not supported "+ format);

    }
    return dataset;
  }

}
