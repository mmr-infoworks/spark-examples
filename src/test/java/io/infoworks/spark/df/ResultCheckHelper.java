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

  /**
   *   TODO ideally we want to compare value of floats , ints etc.
   *   Current I am converting value to string which potentially has issues
   */
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
  public static <T> boolean compareMap(Map<String,T> expected ,Map<String,T> result )  {

    //TODO add special float/decimal check
    return expected.equals(result);
  }

}
