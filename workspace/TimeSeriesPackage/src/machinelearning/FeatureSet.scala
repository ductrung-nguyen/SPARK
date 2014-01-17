package machinelearning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

// This class will load Feature Set from a file
class FeatureSet(val metadataRDD : RDD[String]) extends Serializable {

    private def loadFromFile() = {

        var tags = metadataRDD.take(2).flatMap(line => line.split(",")).toSeq.toList

        // ( index_of_feature, (Feature_Name, Feature_Type))
        //( (0,(Temperature,1))  , (1,(Outlook,1)) ,  (2,(Humidity,1)) , ... )
        (((0 until tags.length / 2) map (index => (tags(index), tags(index + tags.length / 2)))) zip (0 until tags.length))
        .map (x => FeatureInfo(x._1._1, x._1._2, x._2)).toList
    }
    

    lazy val data = loadFromFile()
    lazy val numberOfFeature = data.length
}