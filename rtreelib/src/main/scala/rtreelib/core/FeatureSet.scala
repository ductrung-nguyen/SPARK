package rtreelib.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

// This class will load Feature Set from a file
class FeatureSet(metadata: Array[String]) extends Serializable {

    // map a name of feature to its index
    private var mapNameToIndex: Map[String, Int] = Map[String, Int]() //withDefaultValue -1
    // we can not use withDefaulValue here. It will raise a NotSerializableExecptiopn
    // because of a bug in scala 2.9x. This bug is solved in 2.10

    /**
     * Construct set of features information based on metadata
     */
    private def loadMetadata() = {

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

    /**
     * Get index of a feature based on its name
     */
    def getIndex(name: String): Int = try { mapNameToIndex(name)}  catch { case _ => -1 }
    
    /**
     * List of feature info, we can see it like a map from index to name
     */
    private var rawData = List[FeatureInfo]()
    
    /**
     * Features information
     */
    val data = loadMetadata()
    
    /**
     * Number of features
     */
    lazy val numberOfFeature = data.length     
    
    override def toString() = {
      data.mkString(",\n")
    }
}