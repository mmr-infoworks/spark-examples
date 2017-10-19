package io.infoworks.spark.df;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;

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
  public static Expression parseExpression(SparkSession spark,String expression) {
    return spark.sessionState().sqlParser().parseExpression(expression);
  }

  public static Column getColumnFromExp(SparkSession spark,String expression) {
    return new Column(parseExpression(spark,expression));
  }
}
