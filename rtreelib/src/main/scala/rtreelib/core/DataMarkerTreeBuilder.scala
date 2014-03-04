package rtreelib.core

import org.apache.spark.rdd.RDD

/**
 * This class is representative for each value of each feature in the data set
 * @param index Index of the feature in the whole data set, based zero
 * @param xValue Value of the current feature
 * @param yValue Value of the Y feature associated (target, predicted feature)
 * @param frequency Frequency of this value
 */
class FeatureValueLabelAggregate(index: Int, xValue: Any, yValue: Double, frequency: Int, var label: BigInt = 1)
    extends FeatureValueAggregate(index, xValue, yValue, frequency) {

    /**
     * Sum two FeatureValueAggregates (sum two yValues and two frequencies)
     */
    override def +(that: FeatureValueAggregate) = {
        new FeatureValueLabelAggregate(this.index, this.xValue,
            this.yValue + that.yValue,
            this.frequency + that.frequency,
            this.label)
    }

    override def toString() = "Feature(index:" + index + " | xValue:" + xValue +
        " | yValue" + yValue + " | frequency:" + frequency + " | label:" + label + ")";
}

/**
 * Build tree based on marking label on data.
 * This approach will try to expand all nodes in the same level in one job
 *
 * @param featureSet feature information of input data
 */
class DataMarkerTreeBuilder(_featureSet: FeatureSet) extends TreeBuilder(_featureSet) {

    /**
     * Temporary model file
     */
    val temporaryModelFile = "/tmp/model.temp"
        
    var regions = List[(BigInt, List[Condition])]()

    /**
     * Process a line of data set
     * For each value of each feature, encapsulate it into a FeatureAgregateInfo(fetureIndex, xValue, yValue, frequency)
     *
     * @param line			array of value of each feature in a "record"
     * @param numbeFeatures	the TOTAL number of feature in data set (include features which may be not processed)
     * @param fTypes		type of each feature in each line (in ordered)
     * @return an array of FeatureAggregateInfo, each element is a value of each feature on this line
     */
    protected def processLineWithLabel(line: Array[String], numberFeatures: Int, featureSet: FeatureSet): Array[FeatureValueLabelAggregate] = {
        val length = numberFeatures
        var i = -1;
        Utility.parseDouble(line(yIndex)) match {
            case Some(yValue) => { // check type of Y : if isn't continuous type, return nothing
                try {
                    line.map(f => { // this map is not parallel, it is executed by each worker on their part of the input RDD
                        i = (i + 1) % length
                        if (xIndexes.contains(i)) {
                            featureSet.data(i) match {
                                case nFeature: NumericalFeature => { // If this is a numerical feature => parse value from string to double
                                    val v = Utility.parseDouble(f);
                                    v match {
                                        case Some(d) => new FeatureValueLabelAggregate(i, d, yValue, 1)
                                        case None => throw new Exception("Value of feature " + i + " is not double. Require DOUBLE") //new FeatureValueAggregate(-9, f, 0, 0)
                                    }
                                }
                                // if this is a categorical feature => return a FeatureAggregateInfo
                                case cFeature: CategoricalFeature => new FeatureValueLabelAggregate(i, f, yValue, 1)
                            } // end match fType(i)
                        } // end if
                        else new FeatureValueLabelAggregate(-9, f, 0, -1) // with frequency = -1 and value 0, we will remove unused features
                    }) // end map
                } catch {
                    case e: Exception => { println("Record has some invalid values"); Array[FeatureValueLabelAggregate]() }
                }
            } // end case Some(yvalue)
            case None => { println("Y value is invalid:(%s)".format(line(yIndex))); Array[FeatureValueLabelAggregate]() }
        } // end match Y value
    }

    /**
     * Check a sub data set has meet stop criterion or not
     *
     * @param data data set
     * @return <code>true</code>/<code>false</code> and the average of value of target feature
     */
    def checkStopCriterion(data: Seq[FeatureValueLabelAggregate]): (Boolean, Double) = {

        val yFeature = data.filter(x => x.index == this.yIndex)

        //yFeature.collect.foreach(println)

        val numTotalRecs = yFeature.reduce(_ + _).frequency

        val yValues = yFeature.groupBy(_.yValue)

        val yAggregate = yFeature.map(x => (x.yValue, x.yValue * x.yValue))

        val ySumValue = yAggregate.reduce((x, y) => (x._1 + y._1, x._2 + y._2))

        val EY = ySumValue._1 / numTotalRecs
        val EY2 = ySumValue._2 / numTotalRecs

        val standardDeviation = math.sqrt(EY2 - EY * EY)

        (( // the first component of tuple
            (numTotalRecs <= this.minsplit) // or the number of records is less than minimum
            || (((standardDeviation < this.threshold) && (EY == 0)) || (standardDeviation / EY < this.threshold)) // or standard devariance of values of Y feature is small enough
            ),
            EY // the second component of tuple
            )

    }

    private def selectBestSplitPoint(data: Seq[FeatureValueLabelAggregate], eY: Double): (BigInt, SplitPoint) = {
        val label = data(0).label

        val groupFeatureByIndexAndValue =
            data.groupBy(x => (x.index, x.xValue)) // PM: this operates on an RDD => in parallel

        var featureValueSorted = (
            //data.groupBy(x => (x.index, x.xValue))
            groupFeatureByIndexAndValue // PM: this is an RDD hence you do the map and fold in parallel (in MapReduce this would be the "reducer")
            .map(x => (new FeatureValueAggregate(x._1._1, x._1._2, 0, 0)
                + x._2.foldLeft(new FeatureValueAggregate(x._1._1, x._1._2, 0, 0))(_ + _)))
            // sample results
            //Feature(index:2 | xValue:normal | yValue6.0 | frequency:7)
            //Feature(index:1 | xValue:sunny | yValue2.0 | frequency:5)
            //Feature(index:2 | xValue:high | yValue3.0 | frequency:7)

            .groupBy(x => x.index) // This is again operating on the RDD, and actually is like the continuation of the "reducer" code above
            .map(x =>
                (x._1, x._2.toSeq.sortBy(
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
                        val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency) // number of records
                        val sumY = x._2.foldLeft(0.0)(_ + _.yValue) // total sum of Y

                        var splitPoint: Set[String] = Set[String]()
                        var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0)
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
                        val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency)
                        var currentSumY: Double = 0
                        val sumY = x._2.foldLeft(0.0)(_ + _.yValue)
                        var posibleSplitPoint: Double = 0
                        var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0)
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
            filter(_.index != this.yIndex).
            maxBy(_.weight) // select best feature to split
        // PM: collect here means you're sending back all the data to a single machine (the driver).

        if (splittingPointFeature.index < 0) {
            splittingPointFeature.point = eY
        }

        (label, splittingPointFeature)
    }

    private def updateModel(info: Array[(BigInt, SplitPoint)], isStopNode: Boolean = false) = {
        info.foreach(stoppedRegion =>
            {

                var label = stoppedRegion._1
                var splitPoint = stoppedRegion._2

                println("update model with label=" + label + " splitPoint:" + splitPoint)

                var newnode = (
                    if (isStopNode) {
                        new Empty(splitPoint.point.toString)
                    } else {
                        val chosenFeatureInfoCandidate = featureSet.data.find(f => f.index == splitPoint.index)
                        chosenFeatureInfoCandidate match {
                            case Some(chosenFeatureInfo) => {
                                new NonEmpty(chosenFeatureInfo,
                                    splitPoint.point,
                                    new Empty("empty.left"),
                                    new Empty("empty.right"));
                            }
                            case None => { new Empty(this.ERROR_SPLITPOINT_VALUE) }
                        }
                    }) // end of assign value for new node

                if (newnode.value == this.ERROR_SPLITPOINT_VALUE) {
                    println("Value of job id=" + label + " is invalid")
                } else {

                    // If tree has zero node, create a root node
                    if (treeModel.tree.isEmpty) {
                        treeModel.tree = newnode;

                    } else //  add new node to current model
                    {

                        val level = (Math.log(label.toDouble) / Math.log(2)).toInt
                        var i: Int = level - 1
                        var parent = treeModel.tree; // start adding from root node
                        while (i > 0) {

                            if ((label / (2 << i - 1)) % 2 == 0) {
                                // go to the left
                                parent = parent.left
                            } else {
                                // go go the right
                                parent = parent.right
                            }
                            i -= 1
                        } // end while

                        if (label % 2 == 0) {
                            parent.setLeft(newnode)
                        } else {
                            parent.setRight(newnode)
                        }
                    }
                }
            })
    }

    /**
     * Building tree, bases on:
     *
     * @parm yFeature 	predicted feature
     * @param xFeature	input features
     *
     * @return: <code>TreeModel</code> : root of the tree
     */
    override def startBuildTree(trainingData: RDD[String]) = {

        var rootID = 1
        var expandingNodeIndexes = Set[BigInt]()

        def finish() = {
            expandingNodeIndexes.isEmpty
        }

        // parse raw data
        val mydata = trainingData.map(line => line.split(delimiter))

        // encapsulate each value of each feature in each line into a object
        var transformedData = mydata.map(x => processLineWithLabel(x, featureSet.numberOfFeature, featureSet))

        // filter the 'line' which contains the invalid or missing data
        transformedData = transformedData.filter(x => (x.length > 0)).cache

        // set label for the first job
        // already set by default constructor of class FeatureValueLabelAggregate , so we don't need to put data to regions
        // if this function is called by ContinueFromIncompleteModel, mark the data by the last labels
        markDataByLabel(transformedData, regions)

        // NOTE: label == x, means, this is data used for building node id=x

        var map_label_to_splitpoint = Map[BigInt, SplitPoint]()
        var isError = false;
        
        var iter = 0;

        do {
            iter = iter + 1
            try {
                //if (iter == 3)
                //    throw new Exception("Break for debugging")
                
            	println("NEW ITERATION---------------------")
                
                // save current model before growing tree
                this.treeModel.writeToFile(this.temporaryModelFile)
                

                var data = transformedData.flatMap(x => x.toSeq).filter(_.index >= 0)

                // partitioning data by label
                var groupByLabel = data.groupBy(_.label) // RDD[(BigInt, Seq[FeatureValueLabelAggregate])]

                var checkedStopGroupByLabel = groupByLabel.map(group => (group._1, checkStopCriterion(group._2), group._2))
                // become RDD[BigInt, (Boolean, Double), Seq[FeatureValueLabelAggregate]]
                // == ( label, (isStop, average_of_target_feature), Seq[FeatureValueLabelAggregate])

                // select stopped group
                var stopExpandingGroups = checkedStopGroupByLabel.filter(_._2._1 == true).
                    map(x => (x._1, new SplitPoint(-1, x._2._2, 0))).collect
                // become: Array[(BigInt, SplitPoint)] == Array[(label, SplitPoint)]

                // update model with in-expandable group
                updateModel(stopExpandingGroups, true)

                // select expanding group
                var expandingGroups = checkedStopGroupByLabel.filter(_._2._1 == false).map(x => (x._2._2, x._3))
                // become RDD[(Double,Seq[FeatureValueLabelAggregate])]
                // = RDD[ (average_targetFeature, Seq[FeatureValueLabelAggregate] ) ]

                // select best feature and best split point for this feature on each part of labeled data
                var selectedSplitPoints = expandingGroups.map(x => selectBestSplitPoint(x._2, x._1)).collect
                // become Array[(BigInt, SplitPoint)] 
                // = Array[label, split_point_of_data_with_marked_by_this_label]

                expandingNodeIndexes = Set[BigInt]()
                map_label_to_splitpoint = Map[BigInt, SplitPoint]() withDefaultValue new SplitPoint(-9, 0, 0)

                // process split points
                var validSplitPoint = selectedSplitPoints.filter(_._2.index != -9)

                // select split point of region with has only one feature
                var stoppedSplitPoints = validSplitPoint.filter(_._2.index == -1)

                var nonstoppedSplitPoints = validSplitPoint.filter(_._2.index != -1)

                updateModel(stoppedSplitPoints, true)
                updateModel(nonstoppedSplitPoints, false)

                nonstoppedSplitPoints.foreach(point =>
                    // add expanding Indexes into set
                    {
                        expandingNodeIndexes = expandingNodeIndexes + (point._1)
                        map_label_to_splitpoint = map_label_to_splitpoint + (point._1 -> point._2) // label -> splitpoint
                    })

                println("expandingNodeIndexes:" + expandingNodeIndexes)

                // mark new label for expanding data
                transformedData.foreach(array => {
                    var currentLabel = array(0).label

                    var splitPoint = map_label_to_splitpoint.getOrElse(currentLabel, new SplitPoint(-9, 0, 0))

                    if (splitPoint.index < 0) { // this is stop node
                        array.foreach(element => { element.index = -9 })
                    } else { // this is expanding node => change label of its data
                        splitPoint.point match {
                            // split on numerical feature
                            case d: Double =>
                                {
                                    if (array(splitPoint.index).xValue.asInstanceOf[Double] < splitPoint.point.asInstanceOf[Double]) {
                                        array.foreach(element => element.label = element.label *2)
                                    } else {
                                        array.foreach(element => element.label = element.label *2 + 1)
                                    }
                                }

                            // split on categorical feature    
                            case s: Set[String] =>
                                {
                                    if (splitPoint.point.asInstanceOf[Set[String]].contains(array(splitPoint.index).xValue.asInstanceOf[String])) {
                                        array.foreach(element => element.label = element.label * 2)
                                    } else {
                                        array.foreach(element => element.label = element.label * 2 + 1)
                                    }
                                }
                        }
                    }
                })
            } catch {
                case e: Exception => {
                    isError = true;
                }
            }
        } while (!finish)

        treeModel.isComplete = !isError;

        /* FINALIZE THE ALGORITHM */
        if (!isError) {
            this.treeModel.isComplete = true
            println("\n------------------DONE WITHOUT ERROR------------------\n")
        } else {
            this.treeModel.isComplete = false
            println("\n--------FINISH with some failed jobs at iteration " + iter + " ----------\n")
            println("Temporaty Tree model is stored at " + this.temporaryModelFile + "\n")
        }
    }

    /**
     * Recover, repair and continue build tree from the last state
     *
     * @throw Exception if the tree is never built before
     */
    override def continueFromIncompleteModel(trainingData: RDD[String]) = {
        if (treeModel == null){
            throw new Exception("The tree model is empty because of no building. Please build it first")
        }
        
        if (treeModel.isComplete){
            println("This model is already complete")
        }
        else {
            println("Recover from the last state")   
            /* INITIALIZE */
	        this.featureSet = treeModel.featureSet
	        this.xIndexes = treeModel.xIndexes
	        this.yIndex = treeModel.yIndex
	        
	        startBuildTree(trainingData)
	        
        }
    }

    private def markDataByLabel(data: RDD[Array[FeatureValueLabelAggregate]], regions: List[(BigInt, List[Condition])]) : RDD[Array[FeatureValueLabelAggregate]] = {
        var newdata = 
            if (regions.length > 0) {
            data.map(line => {
                var labeled = false

                // if a line can match one of the Conditions of a region, label it by the ID of this region
                regions.foreach(region => {
                    if (region._2.forall(c => c.check(line(c.splitPoint.index).xValue))) {
                        line.foreach(element => element.label = region._1)
                        labeled = true
                    }
                })
                
                // if this line wasn't marked, it means this line isn't used for building tree
                if (!labeled) line.foreach(element => element.index = -9)
                line
            })
        } 
        else data
        
        newdata
    }
    
     /**
     * Init the last labels from the leaf nodes
     */
    private def initTheLastLabelsFromLeafNodes() = {
        
        var jobIDList = List[(BigInt, List[Condition])]()
        
        def generateJobIter(currentNode : Node, id: BigInt, conditions : List[Condition]) :Unit = {

            if (currentNode.isEmpty && 
                    (currentNode.value == "empty.left" || currentNode.value == "empty.right")){
	                jobIDList = jobIDList :+ (id, conditions)
	            }
            
            if (!currentNode.isEmpty){	// it has 2 children
                    var newConditionsLeft = conditions :+ 
                    		new Condition(new SplitPoint(currentNode.feature.index, currentNode.splitpoint, 0), true)
                    generateJobIter(currentNode.left, id*2, newConditionsLeft)

                    var newConditionsRight = conditions :+ 
                    		new Condition(new SplitPoint(currentNode.feature.index, currentNode.splitpoint, 0), false)
                    generateJobIter(currentNode.right, id*2 + 1, newConditionsRight)
            }    
        }
        
        generateJobIter(treeModel.tree, 1, List[Condition]())
        
        jobIDList.sortBy(-_._1)	// sort jobs by ID descending
        
        var highestLabel = Math.log(jobIDList(0)._1.toDouble)/Math.log(2)
        jobIDList.filter(x => Math.log(x._1.toDouble)/Math.log(2) == highestLabel)
        
        
        regions = jobIDList
        
    }

    override def createNewInstance(featureSet: FeatureSet) = new DataMarkerTreeBuilder(featureSet)
}