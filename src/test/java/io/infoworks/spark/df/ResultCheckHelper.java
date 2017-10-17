package io.infoworks.spark.df;

import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by manoharm on 17/10/17.
 */
public class ResultCheckHelper {

  public static Map<String,String> makeMap(List<Row> rows, List<Integer> keyCols , List<Integer> valueCols) {
    Map<String,String> resultMap = new HashMap<>();
    for (Row row : rows) {
      List<String> keys = keyCols.stream().map(i ->
                          row.get(i)==null? "null" : row.get(i).toString()
                          ).collect(Collectors.toList());
      String key = String.join("$$",keys);
      List<String> values = valueCols.stream().map(i ->
                          row.get(i)==null? "null" : row.get(i).toString()
                          ).collect(Collectors.toList());
      String value = String.join("$$",values);
      resultMap.put(key,value);
    }
    return resultMap;
  }
  public static boolean compareMap(Map<String,String> expected ,Map<String,String> result )  {
    return expected.equals(result);
  }
}
