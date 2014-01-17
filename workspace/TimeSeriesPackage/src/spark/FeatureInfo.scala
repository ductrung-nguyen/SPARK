package spark

import collection.immutable.TreeMap
// xName: Name of feature, such as temperature, weather...
// xType: type of feature: 0 (Continuous) or 1 (Category)
abstract class FeatureInfo(val Name: String, val Type: String, val index: Int) {

    protected var sumY : Double = 0
    protected var number_of_record = 0
        
    override def toString() = "Index:" + index + " | Name: " + Name + " | Type: " + Type;
    
    // Process a value of this feature
    def addValue(value: Any, yValue : Double)
    
    // Get all candidates of splitting points
    def getPossibleSplitPoints() : IndexedSeq[Any]
    
    // Return the index of best split point in PossiblePlitPoints, 
    // the value of left branch's condition, 
    // and the weight of feature if we apply this splitting
    def getBestSplitPoint() : (Int, Any, Double)
    
    def clear()
    
}

object FeatureInfo {
    def apply(Name: String, Type: String, idx: Int) = {
        Type match {
            case "0" => new NumericalFeature(Name, Type, idx)
            case "1" => new CategoricalFeature(Name, Type, idx)
        }
    }
}

case class NumericalFeature(fName: String, fType: String, fIndex: Int) extends FeatureInfo(fName, fType, fIndex) {
    var min: Double = _
    var max: Double = _
    private var isInit = List(0, 0)
    //private var values = Vector()
    private var values = Vector[FeatureValueMap[Double]]()
    private var BestCutPoint : Int = -1
    private var BestTillNow : Double = 0
    
    private val MAX_NUMBER_SPLITPOINT = 10

    override def addValue(value: Any, yValue: Double): Unit = value match {
        case v: Double => {
            insertValue(v, yValue)
            sumY = sumY + yValue
            
            
            
            if (isInit(0) == 0)  { min = v; isInit = isInit.updated(0, 1) }
            else {
                min = if (min > v) v else min
            }

            if (isInit(1) == 0) { max = v ; isInit = isInit.updated(1, 1); }
            else {
                max = if (max < v) v else max
            }
        }

        case s: String => {
            var temp = parseDouble(s)
            temp match {
                case Some(t) => addValue(t, yValue)
            }
        }
    }
    
    private def insertValue(value: Double, yValue: Double) = {
        def insertIter(left: Int, right: Int): Unit = {
        
            if (left <= right && right < values.length) {
                
                val mid = (left + right) / 2
                val midElement = values(mid)
                if (value < midElement.fValue) insertIter(left, mid - 1)
                else if (value > midElement.fValue) insertIter(mid + 1, right)
                else values = values.updated(mid, FeatureValueMap(value, midElement.frequency + 1, midElement.sumYValue + yValue))
            } else {
                
                val (leftPart, rightPart) = values.splitAt(left)
                values = (leftPart :+ FeatureValueMap(value, 1, yValue)) ++ rightPart
            }
        }

        insertIter(0, values.length - 1)
        number_of_record = number_of_record + 1
    }
    
    override def getPossibleSplitPoints() = {
        if (isInit(0) + isInit(1) == 0) Vector() 
        else if (max > min) ((min to max by (max - min)/MAX_NUMBER_SPLITPOINT) map (i => i))
        else Vector(min)
    }
    
    override def getBestSplitPoint() = {
        var sumRight = sumY; var sumLeft : Double= 0
        var nR = number_of_record; var nL : Int = 0
        BestTillNow = -1
        var i = 0
        BestCutPoint = -1
        
        values.map(x => {
            sumLeft = sumLeft + x.sumYValue; sumRight = sumRight - x.sumYValue
            
            nL = nL + x.frequency; nR = nR - x.frequency
            if (nR <= 0) nR = 1
            // if (X[x + 1] > X[i]) always be true, because of TreeMap
            val NewSplitValue = sumLeft*sumLeft/nL + sumRight*sumRight/nR
            
            if (NewSplitValue > BestTillNow) {
                BestTillNow = NewSplitValue
                BestCutPoint = i
            }
            i = i + 1
        })
        if (BestCutPoint < values.length - 1)
        	(BestCutPoint , (values(BestCutPoint).fValue + values(BestCutPoint + 1).fValue)/2, BestTillNow)
        else
            (BestCutPoint , values(BestCutPoint).fValue, BestTillNow)
    }
    
    def clear() = {
        number_of_record = 0
        isInit = List(0, 0)
        		//private var values = Vector()
        values = Vector[FeatureValueMap[Double]]()
        sumY = 0
        BestCutPoint = -1
        BestTillNow = 0
    }
    
    private def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }

    override def toString = "Index:" + index + " | Name: " + Name + " | Type: " + Type +
        " | (Min, Max) = (" + min + "," + max + ")" + " Values: " + values;
}

case class CategoricalFeature(fName: String, fType: String, fIndex: Int) extends FeatureInfo(fName, fType, fIndex) {
    
    private var values = Vector[FeatureValueMap[Int]]()
    private var realValues = Map[String, Int]() withDefaultValue -1
    private var revertRealValue = Map[Int, String]() withDefaultValue "missing"
    private var BestPosition : Int = -1
    private var BestTillNow :Double = 0

    override def addValue(value: Any, yValue : Double) = value match {
        case s: String => { 
        	insertValue(s, yValue)
        	sumY = sumY + yValue
        }
    }
    
    override def getPossibleSplitPoints() = values.map(x => revertRealValue(x.fValue))
    
    override def getBestSplitPoint() = {
        // sort by the average of Y associated to each value
        values = values.sortWith((x, y) => (x.sumYValue/x.frequency < y.sumYValue/y.frequency))
        var sumRight : Double = sumY; var sumLeft : Double = 0
        var nR = number_of_record; var nL : Int = 0
        BestTillNow = 0
        var i = 0
        values.map ( x => {
            
            val YB = x.sumYValue
            val NB = x.frequency
            sumLeft = sumLeft + YB ; sumRight = sumRight - YB
            nL = nL + NB; nR = nR - NB
            //println("FeatureValue:" + revertRealValue(x.fValue) + " SumLeft:" + sumLeft + " sumRight:" + sumRight + " nL:" + nL + " nR:" + nR)
            if (nR <= 0) nR = 1
            
            val NewSplitValue = sumLeft*sumLeft/nL + sumRight*sumRight/nR

            if (NewSplitValue > BestTillNow) {
                BestTillNow = NewSplitValue
                BestPosition = i
            }
            i = i + 1
        })
        
        //val tmp = realValues.map(_ swap)
        
        (BestPosition, 
                values.take(BestPosition + 1).map (x => { revertRealValue(x.fValue) }).toSet,
                BestTillNow)
    }
    
    private def insertValue(valueString: String, yValue: Double) = {
    	var value = realValues(valueString)
    	if (value == -1) value = values.length
 
        def insertIter(left: Int, right: Int): Unit = {
 
            if (left <= right && right < values.length) {
                val mid = (left + right) / 2
                val midElement = values(mid)

                if (value < midElement.fValue) insertIter(left, mid - 1)
                else if (value > midElement.fValue) insertIter(mid + 1, right)
                else values = values.updated(mid, FeatureValueMap(value, midElement.frequency + 1, midElement.sumYValue + yValue))
            } else {
                value = values.length
                realValues = realValues + (valueString -> value)
                revertRealValue = revertRealValue + (value -> valueString)
                val (leftPart, rightPart) = values.splitAt(left)
                values = (leftPart :+ FeatureValueMap(value, 1, yValue)) ++ rightPart
                
            }
        }

        insertIter(0, values.length - 1)
        number_of_record = number_of_record + 1
    }
    
    def clear() = {
        number_of_record = 0
        values = Vector[FeatureValueMap[Int]]()
        realValues = Map[String, Int]() withDefaultValue -1
        revertRealValue = Map[Int, String]() withDefaultValue "missing"
        BestPosition = -1
        BestTillNow  = 0
        sumY = 0
    }

    override def toString = "Index:" + index + " | Name: " + Name + " | Type: " + Type + " | value=(" +
        (values.map(x => x.fValue) mkString " ; ") + " )";
}

// fValue: value of this feature
// frequency : number of record in which features have this value fvalue
// sumYValue : sum of Y's values of records which features have this fvalue
case class FeatureValueMap[+T] (val fValue : T, var frequency : Int, var sumYValue : Double){
}