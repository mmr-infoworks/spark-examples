package io.infoworks.spark.df;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;


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
    Column column1 = emp.col("age").gt(20);
    Column column2 = dept.col("id").equalTo(2);
    Dataset<Row> joined = emp.join(dept.where("id = 2"),column
      ,"left").where(column1);

    Expression joinExpr = spark.sessionState().sqlParser().parseExpression("emp['deptId'] = dept.id and emp.age > 20 ");
    Column joinExprCol = new Column(joinExpr);
    Dataset<Row> joined2 = emp.join(dept,joinExprCol,"left");
    joined.explain(true );
    joined2.explain(true );
    System.out.println("joined using advanced");
    joined2.show();

    System.out.println("joined using simple");
    joined.show();

    //Target.getInstance().write(joined,Format.JSON,output("joinedemp"));

    spark.stop();


  }
}
