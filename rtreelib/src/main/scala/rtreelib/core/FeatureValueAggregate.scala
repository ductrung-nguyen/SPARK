package rtreelib.core

/**
 * This class is representative for each value of each feature in the data set
 * @param index Index of the feature in the whole data set, based zero
 * @param xValue Value of the current feature
 * @param yValue Value of the Y feature associated (target, predicted feature)
 * @param frequency Frequency of this value
 */
class FeatureValueAggregate(val index: Int, var xValue: Any, var yValue: Double, var frequency: Int) extends Serializable {

    /**
     * Increase the frequency of this feature
     * @param acc the accumulator
     */
    def addFrequency(acc: Int): FeatureValueAggregate =
        {
            FeatureValueAggregate.this.frequency = FeatureValueAggregate.this.frequency + acc;
            FeatureValueAggregate.this
        }

    /**
     * Sum two FeatureValueAggregates (sum two yValues and two frequencies)
     */
    def +(that: FeatureValueAggregate) = {
        new FeatureValueAggregate(this.index, this.xValue,
            this.yValue + that.yValue,
            this.frequency + that.frequency)
    }

    override def toString() = "Feature(index:" + index + " | xValue:" + xValue +
        " | yValue" + yValue + " | frequency:" + frequency + ")";
}
