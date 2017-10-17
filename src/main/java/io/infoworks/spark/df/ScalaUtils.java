package io.infoworks.spark.df;

import scala.collection.Seq;

import java.util.List;

/**
 * Created by manoharm on 17/10/17.
 * to convert from java to scala
 */
public class ScalaUtils {

  public static <T> Seq<T> toSeq(List<T> items) {
    return scala.collection.JavaConversions.asScalaBuffer(items).toSeq();
  }
}
