package rtreelib.core
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import rtreelib.core._
import rtreelib.evaluation.Evaluation

object Pruning {
    
    var maxcp = 0.0
    
    private def risk(node : Node) : Double = {
        0.0
    }
    
    /**
     * Prune the full tree to get tree T1 which has the same error rate but smaller size
     */
    private def firstPruneTree(root: Node): Node = {
        def firstPruneTreeIter(node: Node): Node = {
            if (!node.isEmpty) {
                node.setLeft(firstPruneTreeIter(node.left))
                node.setRight(firstPruneTreeIter(node.right))
                val Rl = risk(node.left)
                val Rr = risk(node.right)
                val R = risk(node)
                if (R == (Rl + Rr)) {
                    var newnode = new LeafNode(node.splitpoint.point.toString)
                    newnode.statisticalInformation = node.statisticalInformation
                    newnode
                } else {
                    node
                }
            }else{
            	node
            }
        }

        firstPruneTreeIter(root)
    }
    
    /**
     * Calculate g(t) of every node
     * 
     * @return (id_of_node_will_be_pruned, alpha)
     */
    private def selectNodesToPrune(root : Node, already_pruned_node : Set[BigInt]) : (Set[BigInt], Double) = {	// (prunedIDs, alpha)
        var result = Set[BigInt]()
        var currentMin : Double = Double.MaxValue
        var currentID : BigInt = 0

        def selectNodeToPrunIter(node: Node, id: BigInt): (Double, Int) = { // (riskOfBranch, numberChildren)
            
            // if this is an internal node, and the branch of this node hasn't pruned
            if (!node.isEmpty && !already_pruned_node.contains(id)) {

                var (riskBranchLeft, numLeft) = selectNodeToPrunIter(node.left, (id << 1) )
                var (riskBranchRight, numRight) = selectNodeToPrunIter(node.right, (id << 1) + 1)
                var gT = (risk(node) - (riskBranchLeft + riskBranchRight)) / (numLeft + numRight - 1)

                if (gT == currentMin) {
                    result = result + id
                } 
                else if (gT < currentMin) {
                    result = Set[BigInt]()
                    result = result + id
                    currentMin = gT
                }

                (riskBranchLeft + riskBranchRight, numLeft + numRight)
            } else {
                (risk(node), 1) // one leaf node
            }
        }
        
        selectNodeToPrunIter(root, 0)
        
        // union with the previous set of pruned nodes
        result = result ++ already_pruned_node;
        var pruned_node_list = result.toList.sortWith((a,b) => a < b)
        
        //filter useless pruned nodes
        // for example, if this set contain 3 and 7;
        // 3 is the parent of 7, we don't need to keep 7 in this set
        pruned_node_list.foreach(id => {
            var tmp = id
            while (tmp > 0){
                if (result.contains(tmp >> 1)){
                    result = result - tmp
                    tmp = 0
                }
                tmp = tmp >> 1
            }
        })
        
        
        (result, currentMin)
    }

    private def pruneBranch(root: Node, prunedNodeID: BigInt): Node = {
        var currentid: BigInt = 0

        val level = (Math.log(prunedNodeID.toDouble) / Math.log(2)).toInt
        var i: Int = level - 1
        var parent = root; // start adding from root node
        
        // try to find the parent of pruned node
        while (i > 0) {

            if ((prunedNodeID / (2 << i - 1)) % 2 == 0) {
                // go to the left
                parent = parent.left
            } else {
                // go go the right
                parent = parent.right
            }
            i -= 1
        } // end while

        if (prunedNodeID % 2 == 0) {
            parent.setLeft(parent.left.toLeafNode)
        } else {
            parent.setRight(parent.right.toLeafNode)
        }
        root

    }
    
    
    def getSubTreeSequence(tree : Node) : List[(Set[BigInt], Double)] = {
        var root = firstPruneTree(tree)	// get T1
	    
	    var sequence_alpha_tree = List[(Set[BigInt], Double)]((Set[BigInt](), 0))
	    
	    var finish = false
	    
	    var prunedNodeSet = Set[BigInt]()
	    
	    // SELECT SEQUENCE OF BEST SUB-TREE AND INTERVALS OF ALPHA
	    do {
	        var (nodesNeedToBePruned, alpha) = selectNodesToPrune(root, prunedNodeSet)
	        
	        sequence_alpha_tree = sequence_alpha_tree.:+((nodesNeedToBePruned, alpha))
	        
	        prunedNodeSet = nodesNeedToBePruned
	        
	        finish = (nodesNeedToBePruned.isEmpty)
	    } while (!finish)
	        
	    sequence_alpha_tree    
    }
    
    def getTreeIndexByAlpha(givenAlpha : Double, sequence_tree_alpha : List[(Set[BigInt], Double)]) : Int = {
        var i = 0
        var result : Int = 
        sequence_tree_alpha.indexWhere{
           case (leafNodes, alpha) =>
	        {
	            alpha > givenAlpha
	        }} - 1
        
        result
    }
    /**
     * @param treeModel
     * @param complexityParamter
     * @param dataset
     * @return
     */
    def Prune(treeModel : TreeModel, complexityParamter : Double, dataset: RDD[String]) : TreeModel = {
	    
        this.maxcp = complexityParamter
	    
	    var sequence_alpha_tree = getSubTreeSequence(treeModel.tree) 
	        
	        
	    // CROSS-VALIDATION
	    
	    
	    var N = 10
        var newdata = dataset.mapPartitions(partition => {
            var i = -1
            partition.map(x => {
            	i = (i + 1) % N
            	(i, x)
            })
        })
        
        var yIndex = treeModel.featureSet.getIndex(treeModel.usefulFeatureSet.data(treeModel.yIndex).Name)
        
        for (fold <- (1 to N)){
            var datasetOfThisFold = newdata.filter(x => x._1 != fold).map(x => x._2)
            var testingData = newdata.filter(x => x._1 == fold).map(x => x._2)
            
            val tree = new RegressionTree()
            tree.setDataset(datasetOfThisFold)
            val treeModelOfThisFold = tree.buildTree(treeModel.yFeature, treeModel.xFeatures)
            var tree_broadcast = dataset.context.broadcast(tree)
            
            val sequence_alpha_tree_this_fold = getSubTreeSequence(treeModelOfThisFold.tree)
            var list_subtree_correspoding_to_beta = List[Set[BigInt]]()
            
            for (i <- (0 to sequence_alpha_tree.length -2)){
                val beta = math.sqrt(sequence_alpha_tree(i)._2 * sequence_alpha_tree(i + 1)._2)
                val index = getTreeIndexByAlpha(beta, sequence_alpha_tree_this_fold)
                list_subtree_correspoding_to_beta = list_subtree_correspoding_to_beta.:+(sequence_alpha_tree(index)._1)
            }
            
            var predicted_value_by_subtrees = dataset.map(line => {
	        var record = line.split(",")
	        (
	        		list_subtree_correspoding_to_beta.map(sequence => {
			            tree_broadcast.value.predictOneInstance(record, sequence)
			        }),
	        	record(yIndex)
	        )
            })
            
            val predictRDD = tree.predict(testingData)
            val actualValueRDD = testingData.map(line => line.split(',')(yIndex))	
            val evaluationResult = Evaluation.evaluate(predictRDD, actualValueRDD)
            
        }
	    
	    
	    /*
	    var tree = new RegressionTree()
	    tree.setTreeModel(treeModel)
	    var tree_broadcast = dataset.context.broadcast(tree)
	    var treeModelBroadcast = dataset.context.broadcast(treeModel)
	    var sequence_alpha_tree_broadcast = dataset.context.broadcast(sequence_alpha_tree)
	    var yIndex = treeModel.featureSet.getIndex(treeModel.usefulFeatureSet.data(treeModel.yIndex).Name)
	    
	    var predicted_value_by_subtrees = dataset.map(line => {
	        var record = line.split(",")
	        (
	        sequence_alpha_tree_broadcast.value.map(sequence => {
	            tree_broadcast.value.predictOneInstance(record, sequence._1)
	        }),
	        	record(yIndex)
	        )
	    })
	    // (List[PredictedValues], true_value)
	    
	    
	    //var xIndexes = treeModel.xIndexes.map(index => treeModel.featureSet.getIndex(treeModel.usefulFeatureSet.data(index).Name))
        
	    var predictedRDD = predicted_value_by_subtrees.flatMap(x => {
	        var i = -1
	        x._1.map(element => { i = i + 1; (i, element, x._2) } )
	    })	// RDD[(indexOfSubTree, predictedValue, trueValue)]
	    
	    var validRDD = predictedRDD.filter(v => (!v._2.equals("???") 
            && (Utility.parseDouble(v._3) match {
                case Some(d :Double) => true 
                case None => false 
        }))) // filter invalid record, v._2 is predicted value; v._3 is true value
        
        //var numPartitions = validRDD.partitions.length
        var numInstances = validRDD.count
//        var indexes = dataset.context.parallelize(List.range(0, numInstances.toInt), numPartitions)
        
        var errorMetricRDD = validRDD.map(x => {
            (x._1, ((x._3.toDouble - x._2.toDouble)*(x._3.toDouble - x._2.toDouble), 1))
        })//.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        
        //var squareErrorOfSubTrees = errorMetricRDD.collect.maxBy(x => x._2._1)	// (index, (square_error, numInstances))
        
        
        
        
        var errorMetricRDDWithFoldIndex = errorMetricRDD.mapPartitions(partition => {
            var i = -1
            partition.map(x => { i = (i + 1) % N; ((i, x._1), x._2) }) 
        }, false)	// RDD[((foldTime, indexOfTree), (SquareError, numInstances))]
        
        var errorMetricRDDWithFoldIndexAggregate = 
            errorMetricRDDWithFoldIndex.reduceByKey((a,b) =>{
            (a._1 + b._1, a._2 + b._2)
        }) // RDD[((foldTime, indexOfTree), (SquareError, numInstances))]
        
        
        
        for (fold <- (1 to N)){
            var temp = (errorMetricRDDWithFoldIndexAggregate.filter(x => x._1._1 != fold)
                    .map(x => (x._1._2, (x._2._1, x._2._2))) // RDD[(indexOfTree, (SquareError, numInstances))]
                    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
                    )
            
        }
        
        //var kk = (errorMetricRDDWithFoldIndexAggregate.map(x => (x._1._2, (x._2._1, x._2._2)))
        // RDD[(indexOfTree, (SquareError, numInstances))]
        //		.groupByKey()
        //)
        
        
        
        
        //println("best Pruned SubTree tuple:" + bestPrunedSubTreeTuple)
        
        //var temp = sequence_alpha_tree(bestPrunedSubTreeTuple._1)
        
        //temp._1.foreach( prunedID => root = pruneBranch(root, prunedID))
	    
        //treeModel.tree = root
	    */
        treeModel
	}
}