package rtreelib.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

/**
 * The information of a job, which will find the best feature and the best split point of this feature to split
 * @param id The unique id of jobs
 * @param xConditions The list of condition , which will be used to get the 'right dataset' from the whole dataset
 */
class JobInfo(id: BigInt, xConditions: List[Condition]) extends Serializable {
    
	/**
	 *  unique ID of this job
	 */ 
	var ID = id;
    
    /**
     *  condition of input dataset for this job
     */ 
    var conditions_of_input_dataset = xConditions;

    /**
     *  the node which associate to this job can not expand more ?
     */ 
    var isStopNode = true;
    
    /**
     *  success or error
     */ 
    var isSuccess = false;
    
    /**
     *  error message if job fail
     */ 
    var errorMessage = ""

    /**
     *  best splitpoint (result of this job)
     */ 
    var splitPoint = new SplitPoint(-9, 0, 0);	// -9 is only a mock value
							
    /**
     * Constructor
     */
    def this() = this(0, List[Condition]())
    
    /**
     * Convert this instance into string
     */
    override def toString() = ("Job ID=" + ID.toString + " condition = " + xConditions.toString
            + " splitpoint:" + splitPoint.toString +  "\n")
}

/**
 * This class will launch a job and try to find the best attribute and the best split point for this attribute
 * and submit new job to continue to expand node (if any)
 * 
 * @param job The information of job which will be launched
 * @param inputdata The whole data set
 * @param caller The ThreadTreeBuilder instance, which called instance of this class
 */
class JobExecutor(job: JobInfo, inputData: RDD[Array[FeatureValueAggregate]], 
    caller: ThreadTreeBuilder)
    extends Serializable with Runnable {

    /**
     * Check a sub data set has meet stop criterion or not
     * 
     * @param data data set
     * @return <code>true</code>/<code>false</code> and the average of value of target feature
     */
    def checkStopCriterion(data: RDD[FeatureValueAggregate]): (Boolean, Double) = { //PM: since it operates on RDD it is parallel

        val yFeature = data.filter(x => x.index == caller.yIndex)

        //yFeature.collect.foreach(println)
        
        val sum = yFeature.reduce(_ + _)
        
        val numTotalRecs = sum.frequency
        val sumOfYValue = sum.yValue
        val sumOfYValuePower2 = sum.yValuePower2
        
        val meanY = sumOfYValue / numTotalRecs
        val meanOfYPower2 = sumOfYValuePower2 / numTotalRecs
        val standardDeviation = math.sqrt(meanOfYPower2 - meanY*meanY)
        
        // return tuple (isStop, meanY)
        (( // the first component of tuple
            (numTotalRecs <= caller.minsplit) // or the number of records is less than minimum
            || (((standardDeviation < caller.threshold) && (meanY == 0)) || (standardDeviation / meanY < caller.threshold)) // or standard devariance of values of Y feature is small enough
            ),
            meanY // the second component of tuple
            )
    }

    /**
     * This function will be call when the parent thread start
     */
    @Override
    def run() {
      try{
        println("job executor starts")
        var data = inputData.filter(
            x => job.conditions_of_input_dataset.forall(
                sp => {
                    sp.check(x(sp.splitPoint.index).xValue)
                })).flatMap(x => x.toSeq)

        println("after checking stop condition")        
        val (stopExpand, eY) = checkStopCriterion(data)
        if (stopExpand) {

            var splittingPointFeature = new SplitPoint(-1, eY, 1)	// splitpoint for left node
                job.splitPoint = splittingPointFeature
            	job.isSuccess =  true;
                caller.addJobToFinishedQueue(job)
        } else {
            val groupFeatureByIndexAndValue =
                data.groupBy((x : FeatureValueAggregate) => (x.index, x.xValue), 20) // PM: this operates on an RDD => in parallel

            println("after group feature by index and value")
            var featureValueSorted = (
                //data.groupBy(x => (x.index, x.xValue))
                groupFeatureByIndexAndValue // PM: this is an RDD hence you do the map and fold in parallel (in MapReduce this would be the "reducer")

                .map(x => x._2.reduce((f1,f2) => f1 + f2))
                // sample results
                //Feature(index:2 | xValue:normal | yValue6.0 | frequency:7)
                //Feature(index:1 | xValue:sunny | yValue2.0 | frequency:5)
                //Feature(index:2 | xValue:high | yValue3.0 | frequency:7)

                .groupBy((x : FeatureValueAggregate) => x.index, 20) // This is again operating on the RDD, and actually is like the continuation of the "reducer" code above
                .map(x =>
                    (x._1, x._2.sortBy(
                        v => v.xValue match {
                            case d: Double => d // sort by xValue if this is numerical feature
                            case s: String => v.yValue / v.frequency // sort by the average of Y if this is categorical value
                        }))))

            println("after sort feature by some criterions")
            var splittingPointFeature = featureValueSorted.map(x => // operates on an RDD, so this is in parallel
                x._2(0).xValue match {
                    case s: String => // process with categorical feature
                        {
                            println ("matching case string")
                            var allValues = x._2
                            if (allValues.length == 1){
                                new SplitPoint(-1, 0.0, 0.0)	// sign of stop node
                            }
                            else {
                                
                                var currentSumY: Double = 0 // current sum of Y of elements on the left of current feature
	                            var temp = allValues.reduce((f1, f2) => f1 + f2)
	                            val numRecs = temp.frequency
	                            val sumY = temp.yValue
	                            var splitPointIndex: Int = 0
	                            var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0, 0)
	                            var acc: Int = 0
	                            println("before mapping to get all possible splitpoints")
	                            var bestSplitPoint = new SplitPoint(x._1, splitPointIndex, 0)
                                var maxWeight = Double.MinValue
                                var currentWeight : Double = 0
                                
                                x._2.foreach(f => {

                                    if (lastFeatureValue.index == -1) {
                                        lastFeatureValue = f
                                    } else {
                                        currentSumY = currentSumY + lastFeatureValue.yValue
                                        splitPointIndex = splitPointIndex + 1
                                        acc = acc + lastFeatureValue.frequency
                                        currentWeight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                        lastFeatureValue = f
                                        if (currentWeight > maxWeight){
                                            bestSplitPoint.point = splitPointIndex
                                            bestSplitPoint.weight = currentWeight
                                            maxWeight = currentWeight
                                        }
                                    }
                                })
                                
                                var splitPointValue = x._2.map(f => f.xValue).take(splitPointIndex).toSet
                                bestSplitPoint.point = splitPointValue
                                bestSplitPoint
                            /*
                            	val allValues2 = allValues.drop(1)
                            	allValues = allValues.dropRight(1)
                            	val pairsOfValues = allValues zip allValues2
                            	
                            	var splitPointIndex: Int = 0
                            	var acc: Int = 0
                            	
                            	var bestSplitPoint = pairsOfValues.map( pair => { 
                            	    splitPointIndex = splitPointIndex + 1
                            	    currentSumY = currentSumY + pair._1.yValue
                                    acc = acc + pair._1.frequency
                                    val weight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                    new SplitPoint(x._1, splitPointIndex, weight)
                            	}).maxBy(_.weight)
                            	
                            	var splitPointValue =  
                            	*/
                            }
                            
                            
                        }
                    case d: Double => // process with numerical feature
                        {
                            println ("matching case double")
                            var acc: Int = 0 // number of records on the left of the current element
                            var currentSumY: Double = 0
                            //val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency)
                            //val sumY = x._2.foldLeft(0.0)(_ + _.yValue)
                            var temp = x._2.reduce((f1, f2) => f1 + f2)
                            val numRecs = temp.frequency
                            val sumY = temp.yValue
                            
                            var posibleSplitPoint: Double = 0
                            var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0, 0)
                            
                            var bestSplitPoint = new SplitPoint(x._1, posibleSplitPoint, 0)
                            var maxWeight = Double.MinValue
                            var currentWeight : Double = 0
                            
                            println("before mapping to get all possible splitpoints")
                            var allValues = x._2
                            if (allValues.length == 1){
                                new SplitPoint(-1, 0.0, 0.0)	// sign of stop node
                            }
                            else {
                                x._2.foreach(f => {

                                    if (lastFeatureValue.index == -1) {
                                        lastFeatureValue = f
                                    } else {
                                        posibleSplitPoint = (f.xValue.asInstanceOf[Double] + lastFeatureValue.xValue.asInstanceOf[Double]) / 2;
                                        currentSumY = currentSumY + lastFeatureValue.yValue
                                        acc = acc + lastFeatureValue.frequency
                                        currentWeight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                        lastFeatureValue = f
                                        if (currentWeight > maxWeight){
                                            bestSplitPoint.point = posibleSplitPoint
                                            bestSplitPoint.weight = currentWeight
                                            maxWeight = currentWeight
                                        }
                                    }
                                })
                                bestSplitPoint
                            }
                        } // end of matching double
                } // end of matching xValue
                ).
                filter(_.index != caller.yIndex).collect.
                maxBy(_.weight) // select best feature to split
            // PM: collect here means you're sending back all the data to a single machine (the driver).

            println("after finding best split point")
            if (splittingPointFeature.index < 0) { 
                splittingPointFeature.point = eY
                job.splitPoint = splittingPointFeature
                job.isSuccess = true
                if (splittingPointFeature.index == -9)	// this is unused feature, so ignore it when updating model
                    job.isStopNode = false
                    
                caller.addJobToFinishedQueue(job)
            } else {
                //val chosenFeatureInfo = caller.featureSet.data.filter(f => f.index == splittingPointFeature.index).take(0)

                val leftCondition = job.conditions_of_input_dataset :+ new Condition(splittingPointFeature, true)
                val rightCondition = job.conditions_of_input_dataset :+ new Condition(splittingPointFeature, false)
                val leftJob = new JobInfo(job.ID << 1, leftCondition)
                val rightJob = new JobInfo((job.ID << 1) + 1, rightCondition)
                
                
                job.splitPoint = splittingPointFeature
                job.isStopNode = false
                job.isSuccess = true
                caller.addJobToFinishedQueue(job)
                
                caller.addJobToExpandingQueue(leftJob)
                caller.addJobToExpandingQueue(rightJob)
                
                
            } // end of if index == -1
        }
        
      }
      catch{
        case e: Exception => {
          job.errorMessage = e.getMessage()
          job.isSuccess = false;
          caller.addJobToFinishedQueue(job)
          e.printStackTrace()
        }
      }

    }

}
