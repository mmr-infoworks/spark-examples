package io.infoworks.spark.df;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by manoharm on 16/10/17.
 */
public class DFTest {
  public static String input(String file) {
    return "df_integ_spark/examples/input/"+file;
  }
  public static String output(String file) {
    return "df_integ_spark/examples/output/"+file;
  }
  public static void main(String[] args) {

    SparkSession spark = DFSparkContext.spark;
    Dataset<Row> emp = Source.getInstance().getSource(Format.JSON,input("emp.json"));
    Dataset<Row> dept = Source.getInstance().getSource(Format.JSON,input("dept.json"));
    Dataset<Row> filtered = emp.filter(" trim(name) =  'Andy'");
    //Target.getInstance().write(filtered,Format.JSON,"/Users/manoharm/infoworks/spark/examples/output/filteredemp.json");
    Column column = emp.col("deptId").equalTo(dept.col("id"));
    Column column1 = emp.col("age").gt(19);
    Column column2 = dept.col("id").equalTo(2);
    Dataset<Row> joined = emp.join(dept.where("id = 2"),column
      ,"left").where(column1);
    joined.explain(true );

    Target.getInstance().write(joined,Format.JSON,output("joinedemp"));

    spark.stop();


  }
}
