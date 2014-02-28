package rtreelib

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

// This class will load Feature Set from a file
class FeatureSet(metadata: Array[String]) extends Serializable {

    private var mapNameToIndex: Map[String, Int] = Map[String, Int]() //withDefaultValue -1
    // we can not use withDefaulValue here. It will raise a NotSerializableExecptiopn
    // because of a bug in scala 2.9x. This bug is solved in 2.10

    private def loadFromFile() = {

        if (metadata.length >= 2){
	        var tags = metadata.take(2).flatMap(line => line.split(",")).toSeq.toList
	
	        // ( index_of_feature, (Feature_Name, Feature_Type))
	        //( (0,(Temperature,1))  , (1,(Outlook,1)) ,  (2,(Humidity,1)) , ... )
	        val data = (((0 until tags.length / 2) map (index => (tags(index), tags(index + tags.length / 2)))) zip (0 until tags.length))
	            .map(x => FeatureInfo(x._1._1, x._1._2, x._2)).toList
	        data.foreach(x => { mapNameToIndex = mapNameToIndex + (Utility.normalizeString(x.Name) -> x.index) })
	        data
        }
        else {
            List[FeatureInfo]()
        }
    }

    def getIndex(name: String): Int = try { mapNameToIndex(name)}  catch { case _ => -1 }
    
    var rawData = List[FeatureInfo]()
    val data = loadFromFile()
    
    lazy val numberOfFeature = data.length     
    
    override def toString() = {
      data.mkString(",\n")
    }
}