package io.infoworks.spark.df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by manoharm on 16/10/17.
 */
public class Target {
  SparkSession spark = DFSparkContext.spark;

  private static  Target INSTANCE = new Target();
  private  Target() {

  }
  public static Target getInstance(){
    return INSTANCE;
  }

  public void write(Dataset<Row> dataSet, Format format, String path) {
    switch (format) {
      case JSON:
        dataSet.write().format("json").json(path);
        break;
      default:
        throw new RuntimeException(" format not supported "+ format);

    }
  }
}
