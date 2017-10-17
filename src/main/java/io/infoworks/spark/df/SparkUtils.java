package io.infoworks.spark.df;

import org.apache.spark.sql.Column;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by manoharm on 17/10/17.
 * to convert from ordinary strings to DataFrame specific stuff
 */
public class SparkUtils {

  public static Column toColumn(String column) {
    return new Column(column);
  }

  public static List<Column> toColumns(List<String> columns) {
    return columns.stream().map(i -> toColumn(i)).collect(Collectors.toList());
  }
}
