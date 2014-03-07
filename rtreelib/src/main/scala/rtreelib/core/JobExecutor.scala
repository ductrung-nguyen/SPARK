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
          
        var data = inputData.filter(
            x => job.conditions_of_input_dataset.forall(
                sp => {
                    sp.check(x(sp.splitPoint.index).xValue)
                })).flatMap(x => x.toSeq)

        val (stopExpand, eY) = checkStopCriterion(data)
        if (stopExpand) {

            var splittingPointFeature = new SplitPoint(-1, eY, 1)	// splitpoint for left node
                job.splitPoint = splittingPointFeature
            	job.isSuccess =  true;
                caller.addJobToFinishedQueue(job)
        } else {
            val groupFeatureByIndexAndValue =
                data.groupBy(x => (x.index, x.xValue)) // PM: this operates on an RDD => in parallel

            var featureValueSorted = (
                //data.groupBy(x => (x.index, x.xValue))
                groupFeatureByIndexAndValue // PM: this is an RDD hence you do the map and fold in parallel (in MapReduce this would be the "reducer")
                /*.map(x => (new FeatureValueAggregate(x._1._1, x._1._2, 0, 0)
                    + x._2.foldLeft(new FeatureValueAggregate(x._1._1, x._1._2, 0, 0))(_ + _)))
                */
                .map(x => x._2.reduce((f1,f2) => f1 + f2))
                // sample results
                //Feature(index:2 | xValue:normal | yValue6.0 | frequency:7)
                //Feature(index:1 | xValue:sunny | yValue2.0 | frequency:5)
                //Feature(index:2 | xValue:high | yValue3.0 | frequency:7)

                .groupBy(x => x.index) // This is again operating on the RDD, and actually is like the continuation of the "reducer" code above
                .map(x =>
                    (x._1, x._2.sortBy(
                        v => v.xValue match {
                            case d: Double => d // sort by xValue if this is numerical feature
                            case s: String => v.yValue / v.frequency // sort by the average of Y if this is categorical value
                        }))))

            var splittingPointFeature = featureValueSorted.map(x => // operates on an RDD, so this is in parallel
                x._2(0).xValue match {
                    case s: String => // process with categorical feature
                        {
                            var acc: Int = 0; // the number records on the left of current feature
                            var currentSumY: Double = 0 // current sum of Y of elements on the left of current feature
                            var temp = x._2.reduce((f1, f2) => f1 + f2)
                            val numRecs = temp.frequency
                            val sumY = temp.yValue
                            //val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency) // number of records
                            //val sumY = x._2.foldLeft(0.0)(_ + _.yValue) // total sum of Y

                            var splitPoint: Set[String] = Set[String]()
                            var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0, 0)
                            try {
                                x._2.map(f => {

                                    if (lastFeatureValue.index == -1) {
                                        lastFeatureValue = f
                                        new SplitPoint(x._1, Set(), 0.0)
                                    } else {
                                        currentSumY = currentSumY + lastFeatureValue.yValue
                                        splitPoint = splitPoint + lastFeatureValue.xValue.asInstanceOf[String]
                                        acc = acc + lastFeatureValue.frequency
                                        val weight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                        lastFeatureValue = f
                                        new SplitPoint(x._1, splitPoint, weight)
                                    }
                                }).drop(1).maxBy(_.weight) // select the best split // PM: please explain this trick with an example
                                // we drop 1 element because with Set{A,B,C} , the best split point only be {A} or {A,B}
                            } catch {
                                case e: UnsupportedOperationException => new SplitPoint(-1, 0.0, 0.0)
                            }
                        }
                    case d: Double => // process with numerical feature
                        {
                            var acc: Int = 0 // number of records on the left of the current element
                            var currentSumY: Double = 0
                            //val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency)
                            //val sumY = x._2.foldLeft(0.0)(_ + _.yValue)
                            var temp = x._2.reduce((f1, f2) => f1 + f2)
                            val numRecs = temp.frequency
                            val sumY = temp.yValue
                            
                            var posibleSplitPoint: Double = 0
                            var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0, 0)
                            try {
                                x._2.map(f => {

                                    if (lastFeatureValue.index == -1) {
                                        lastFeatureValue = f
                                        new SplitPoint(x._1, 0.0, 0.0)
                                    } else {
                                        posibleSplitPoint = (f.xValue.asInstanceOf[Double] + lastFeatureValue.xValue.asInstanceOf[Double]) / 2;
                                        currentSumY = currentSumY + lastFeatureValue.yValue
                                        acc = acc + lastFeatureValue.frequency
                                        val weight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                        lastFeatureValue = f
                                        new SplitPoint(x._1, posibleSplitPoint, weight)
                                    }
                                }).drop(1).maxBy(_.weight) // select the best split
                            } catch {
                                case e: UnsupportedOperationException => new SplitPoint(-1, 0.0, 0.0)
                            }
                        } // end of matching double
                } // end of matching xValue
                ).
                filter(_.index != caller.yIndex).collect.
                maxBy(_.weight) // select best feature to split
            // PM: collect here means you're sending back all the data to a single machine (the driver).

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
