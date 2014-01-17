package spark

import collection.immutable.TreeMap
import org.apache.spark._
import org.apache.spark.SparkContext._

object TestingWorkSheet {

    class FeatureAggregateInfo(val index: Int, var xValue: Any, var yValue: Double, var frequency: Int) extends Serializable {
        def addFrequency(acc: Int): FeatureAggregateInfo = { this.frequency = this.frequency + acc; this }
        def +(that: FeatureAggregateInfo) = {
            this.frequency = this.frequency + that.frequency
            this.yValue = this.yValue + that.yValue
            this
        }
        override def toString() = "Feature(index:" + index + " | xValue:" + xValue +
            " | yValue" + yValue + " | frequency:" + frequency + ")";
    }

    case class FeatureSet(file: String, val context: SparkContext) {
        def this(file: String) = this(file, new SparkContext("local", "SparkContext"))
        private def loadFromFile() = {

            //val input_fileName: String = "/home/loveallufev/semester_project/input/small_input";
            val myTagInputFile = context.textFile(file, 1)

            var tags = myTagInputFile.take(2).flatMap(line => line.split(",")).toSeq.toList

            // ( index_of_feature, (Feature_Name, Feature_Type))
            //( (0,(Temperature,1))  , (1,(Outlook,1)) ,  (2,(Humidity,1)) , ... )
            (((0 until tags.length / 2) map (index => (tags(index), tags(index + tags.length / 2)))) zip (0 until tags.length))
                .map(x => FeatureInfo(x._1._1, x._1._2, x._2)).toList
        }

        lazy val data = loadFromFile()
        lazy val numberOfFeature = data.length
    };import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(1706); 

    def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None };System.out.println("""parseDouble: (s: String)Option[Double]""");$skip(60); 
    val context = new SparkContext("local", "SparkContext");System.out.println("""context  : org.apache.spark.SparkContext = """ + $show(context ));$skip(1219); 
    
    def processLine(line: Array[String], numberFeatures: Int, fTypes: Vector[String]): org.apache.spark.rdd.RDD[FeatureAggregateInfo] = {
        val length = numberFeatures
        var i = -1;
        parseDouble(line(length - 1)) match {
            case Some(yValue) => { // check type of Y : if isn't continuos type, return nothing
                context.parallelize(line.map(f => {
                    i = (i + 1) % length
                    fTypes(i) match {
                        case "0" => {	// If this is a numerical feature => parse value from string to double
                            val v = parseDouble(f);
                            v match {
                                case Some(d) => new FeatureAggregateInfo(i, d, yValue, 1)
                                case None => new FeatureAggregateInfo(-1, f, 0, 0)
                            }
                        }
                        // if this is a categorial feature => return a FeatureAggregateInfo
                        case "1" => new FeatureAggregateInfo(i, f, yValue, 1)
                    }
                }))
            }
            //case None => org.apache.spark.rdd.RDD[FeatureAggregateInfo]()
        }

    };System.out.println("""processLine: (line: Array[String], numberFeatures: Int, fTypes: Vector[String])org.apache.spark.rdd.RDD[spark.TestingWorkSheet.FeatureAggregateInfo]""");$skip(90); 

    
    
    val dataInputURL = "/home/loveallufev/semester_project/input/small_input2";System.out.println("""dataInputURL  : java.lang.String = """ + $show(dataInputURL ));$skip(107); 

    var featureSet = new FeatureSet("/home/loveallufev/semester_project/input/tag_small_input2", context);System.out.println("""featureSet  : spark.TestingWorkSheet.FeatureSet = """ + $show(featureSet ));$skip(53); 
		val myDataFile = context.textFile(dataInputURL, 1);System.out.println("""myDataFile  : org.apache.spark.rdd.RDD[String] = """ + $show(myDataFile ));$skip(77); 
    var myDataFile2 = scala.io.Source.fromFile(dataInputURL).getLines.toList;System.out.println("""myDataFile2  : List[String] = """ + $show(myDataFile2 ));$skip(58); 

    var mydata = myDataFile.map(line => line.split(","));System.out.println("""mydata  : org.apache.spark.rdd.RDD[Array[java.lang.String]] = """ + $show(mydata ));$skip(54); 
    val number_of_features = mydata.take(1)(0).length;System.out.println("""number_of_features  : Int = """ + $show(number_of_features ));$skip(76); 
    val featureTypes = Vector[String]() ++ featureSet.data.map(x => x.Type);System.out.println("""featureTypes  : scala.collection.immutable.Vector[String] = """ + $show(featureTypes ));$skip(85); 
    val aggregateData = mydata.map(processLine(_, number_of_features, featureTypes));System.out.println("""aggregateData  : org.apache.spark.rdd.RDD[org.apache.spark.rdd.RDD[spark.TestingWorkSheet.FeatureAggregateInfo]] = """ + $show(aggregateData ));$skip(38); 
    println(buildTree(aggregateData));$skip(7158); 

    //def buildTree(data: List[FeatureAggregateInfo]): Unit = {

		def buildTree(data: org.apache.spark.rdd.RDD[org.apache.spark.rdd.RDD[FeatureAggregateInfo]]): Node = {
				
				var yFeature = data.map(x => x.filter(y => (y.index == number_of_features - 1)).first ).groupBy(x => x.index).take(1)(0)
				if (yFeature._2.length == 1) new Empty(yFeature._2(0).toString)
				
        var featureValueSorted = (data.reduce(_ union _)
        													.groupBy(x => (x.index, x.xValue))
            .map(x => (new FeatureAggregateInfo(x._1._1, x._1._2, 0, 0)
                + x._2.foldLeft(new FeatureAggregateInfo(x._1._1, x._1._2, 0, 0))(_ + _)))
            /*
                																	Feature(index:2 | xValue:normal | yValue6.0 | frequency:7)
                                                  Feature(index:1 | xValue:sunny | yValue2.0 | frequency:5)
                                                  Feature(index:4 | xValue:14.5 | yValue1.0 | frequency:1)
                                                  Feature(index:2 | xValue:high | yValue3.0 | frequency:7)
                  */
            .groupBy(x => x.index)
            .map(x =>
            	(x._1, x._2.toList.sortBy(
            		v => v.xValue match {
	                case d: Double => d // sort by xValue if this is numerical feature
	                case s: String => v.yValue / v.frequency // sort by the average of Y if this is categorical value
            		})
            	))
            	)
         
         var splittingPointFeature = featureValueSorted.map(x =>
            				(	x._1,
            					x._2(0).xValue match {
            						case s: String => // process with categorical feature
            							//x._2.map (f => f)
	        							{
	        								var acc: Int = 0; // the number records on the left of current feature
	        								var currentSumY : Double = 0	// current sum of Y of elements on the left of current feature
	        								val numRecs : Int = x._2.foldLeft(0)(_ + _.frequency)	// number of records
	        								val sumY = x._2.foldLeft(0.0)(_ + _.yValue)	// total sum of Y
	        								var splitPoint: Set[String] = Set[String]()
	        								var lastFeatureValue = new FeatureAggregateInfo(-1,0,0,0)
	        								x._2.map(f => {
	        																
	        																if (lastFeatureValue.index == -1){
	        																	lastFeatureValue = f
	        																	(0.0,0.0)
	        																}else {
	        																	currentSumY = currentSumY + lastFeatureValue.yValue
	        																	splitPoint = splitPoint + lastFeatureValue.xValue.asInstanceOf[String]
	        																	acc = acc + lastFeatureValue.frequency
	        																	val weight = currentSumY*currentSumY/acc + (sumY - currentSumY)*(sumY - currentSumY)/(numRecs - acc)
	        																	lastFeatureValue = f
	        																	(splitPoint, weight)
	        																}
	        															}
	        												).drop(1).maxBy(_._2) // select the best split
	        							}
            						case d: Double => // process with numerical feature
            						{
            							var acc: Int = 0	// number of records on the left of the current element
            							val numRecs : Int = x._2.foldLeft(0)(_ + _.frequency)
            							var currentSumY : Double = 0
            							val sumY = x._2.foldLeft(0.0)(_ + _.yValue)
            							var posibleSplitPoint : Double = 0
            							var lastFeatureValue = new FeatureAggregateInfo(-1,0,0,0)
            							x._2.map (f => {
            							
            														if (lastFeatureValue.index == -1){
            															lastFeatureValue = f
            															(0.0,0.0)
            														}
            														else {
            															posibleSplitPoint = (f.xValue.asInstanceOf[Double] + lastFeatureValue.xValue.asInstanceOf[Double])/2;
            															currentSumY = currentSumY + lastFeatureValue.yValue
            															acc = acc + lastFeatureValue.frequency
            															val weight = currentSumY*currentSumY/acc + (sumY - currentSumY)*(sumY - currentSumY)/(numRecs - acc)
	        																lastFeatureValue = f
	        																(posibleSplitPoint, weight)
            														}
	            													
            													}).drop(1).maxBy(_._2)	// select the best split
            						}	// end of matching double
            					}	// end of matching xValue
            				)	// end of pair
            		).filter(_._1 != number_of_features - 1).collect.toList.maxBy(_._2._2)	// select best feature to split
        
        val chosenFeatureInfo = featureSet.data.filter(f => f.index == splittingPointFeature._1)(0)
        
        splittingPointFeature match {
        	case fs : (Int, (Set[String], Double)) => {	// split on categorical feature
        		//val left = data filter (x => fs._2._1.contains(x(chosenFeatureInfo.index).xValue.asInstanceOf[String]))
            //val right = data filter (x => !fs._2._1.contains(x(chosenFeatureInfo.index).xValue.asInstanceOf[String]))
            val left = data.filter {
            	x => (
            	x.filter( y => ( y.index == chosenFeatureInfo.index && fs._2._1.contains(y.xValue.asInstanceOf[String]))).count > 0
            	)
            }
            val right = data.filter {
            	x => (
            	x.filter( y => ( y.index == chosenFeatureInfo.index && fs._2._1.contains(y.xValue.asInstanceOf[String]))).count == 0
            	)
            }
                    new NonEmpty(
                        chosenFeatureInfo, // featureInfo
                        (fs._2._1.toString, "Not in " + fs._2._1.toString), // left + right conditions
                        buildTree(left), // left
                        buildTree(right) // right
                        )
        	}
        	case fd : (Int, (Double, Double)) => {	// split on numerical feature
        		//val left = data filter (x => (x(chosenFeatureInfo.index).xValue.asInstanceOf[Double] < fd._2._1))
            //val right = data filter (x => (x(chosenFeatureInfo.index).xValue.asInstanceOf[Double] >= fd._2._1))
            val left = data.filter {
            	x => (
            	x.filter( y => ( y.index == chosenFeatureInfo.index &&  y.xValue.asInstanceOf[Double] < fd._2._1)).count > 0
            	)
            }
            val right = data.filter {
            	x => (
            	x.filter( y => ( y.index == chosenFeatureInfo.index &&  y.xValue.asInstanceOf[Double] < fd._2._1)).count == 0
            	)
            }
        		new NonEmpty(
                        chosenFeatureInfo, // featureInfo
                        (chosenFeatureInfo.Name + " < " + fd._2._1, chosenFeatureInfo.Name + " >= " + fd._2._1), // left + right conditions
                        buildTree(left), // left
                        buildTree(right) // right
                        )
        	}
        	
        }
        //splittingPointFeature.foreach(x => println(x))
        //println(tmp.mkString("***"))
    };System.out.println("""buildTree: (data: org.apache.spark.rdd.RDD[org.apache.spark.rdd.RDD[spark.TestingWorkSheet.FeatureAggregateInfo]])spark.Node""")}

}