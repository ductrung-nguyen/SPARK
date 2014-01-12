package spark


import org.apache.spark._
import org.apache.spark.SparkContext._

// xName: Name of feature, such as temperature, weather...
// xType: type of feature: 0 (Continuous) or 1 (Category)
case class FeatureInfo(val Name:String, val Type:Int){
    override def toString() = "Name: " + Name + " | Type: " + Type;
}

object RegressionTree {
  val x = FeatureInfo("Weather", 1).toString
}