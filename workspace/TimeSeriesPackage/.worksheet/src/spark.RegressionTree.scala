package spark

import org.apache.spark._
import org.apache.spark.SparkContext._

object RegressionTree {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(169); 
    lazy val context = new SparkContext("local", "SparkContext");System.out.println("""context  : org.apache.spark.SparkContext = <lazy>""");$skip(78); 
    val dataInputURL = "/home/loveallufev/semester_project/input/small_input";System.out.println("""dataInputURL  : java.lang.String = """ + $show(dataInputURL ));$skip(105); 
    var featureSet = new FeatureSet("/home/loveallufev/semester_project/input/tag_small_input", context);System.out.println("""featureSet  : spark.FeatureSet = """ + $show(featureSet ));$skip(61); 
    

    val myDataFile = context.textFile(dataInputURL, 1);System.out.println("""myDataFile  : org.apache.spark.rdd.RDD[String] = """ + $show(myDataFile ));$skip(38); 

    featureSet.data.foreach(println);$skip(37); 
    featureSet.data.foreach(println);$skip(56); 

    var data = myDataFile.map(line => line.split(","));System.out.println("""data  : org.apache.spark.rdd.RDD[Array[java.lang.String]] = """ + $show(data ));$skip(26); 
    data.foreach(println);$skip(59); 
    for (line <- data; field <- line ) {
			println(field)}
			//featureinfo.addValue(field)
			
		
    }

}