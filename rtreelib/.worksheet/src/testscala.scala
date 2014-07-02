
import scala.util.Random
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

    class Base {
    	def println = "Hello"
    }
class Ex1 extends Base {
    	override def println = "Hello1"
    }
    
    class Ex2 extends Base {
    	override def println = "Hello2"
    }

object testscala {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(828); 

    def calculateCorrelation[T : Manifest](data: RDD[Array[T]]) = {
			val pair = data.flatMap(array => {
				var temp = List [((T,T), Int)]()
				for (i <- 0 until array.length - 2) {
						temp = temp.+:((array(i), array(i+1)), 1)
						temp = temp.+:((array(i + 1), array(i)), 1)
				}
				temp
			})
			
			val weights = (pair.reduceByKey(_+_)
			.map {
				case x => (x._1._1, (x._1._2, x._2))
			}.groupByKey.flatMap(x => x._2).reduceByKey(_ + _)
			)
			
			weights.collect
			
    }
    
    
    
    implicit class IntWithTimes(x: Int) {
    def times[A](f: => A): Unit = {
    	println("Type of A:"+ f )
      def loop(current: Int): Unit =
        if(current > 0) {
          f
          loop(current - 1)
        }
      loop(x)
    }
  }
  
  implicit class Ex1Imp(x: Ex1) {
    def times[A](f: => A): Unit = {
    	println(x.println)
    }
  }
  
  implicit class BaseImp(x: Base) {
    def times[A](f: => A): Unit = {
    	println("base")
    	println(x.println)
    }
  };System.out.println("""calculateCorrelation: [T](data: org.apache.spark.rdd.RDD[Array[T]])(implicit evidence$1: Manifest[T])Array[(T, Int)]""");$skip(527); 

    var x: Base = new Ex1();System.out.println("""x  : Base = """ + $show(x ));$skip(38); 
    x.asInstanceOf[Ex1] times println

    abstract class Pruning[+T] {
        def prune = ""
    };$skip(174); 
    
    implicit def pruneForGeneric[T] = new Pruning[T] {
    	override def prune : String = "Generic"
    };System.out.println("""pruneForGeneric: [T]=> testscala.Pruning[T]""");$skip(99); 
    
    implicit def pruneForID3 = new Pruning[Ex1] {
    	override def prune = "prune ID3"
    };System.out.println("""pruneForID3: => testscala.Pruning[Ex1]""");$skip(101); 
    
    implicit def pruneForCART = new Pruning[Ex2] {
    	override def prune = "prune CART"
    };System.out.println("""pruneForCART: => testscala.Pruning[Ex2]""");$skip(73); 
    
    def prune[T <: Base](a : T )(implicit g : Pruning[T]) = g.prune;System.out.println("""prune: [T <: Base](a: T)(implicit g: testscala.Pruning[T])String""");$skip(11); val res$0 = 
    0 to 1;System.out.println("""res0: scala.collection.immutable.Range.Inclusive = """ + $show(res$0));$skip(36); val res$1 = 
    
    "ad,bs,cdfa,dd".split(",");System.out.println("""res1: Array[String] = """ + $show(res$1))}
    
    //prune(x)
    //x.isInstanceOf[Ex2]
  
  /*
  implicit def a[T] =new X[T] { def id =println("generic") }
  implicit def b =new X[Int] { def id =println("Int") }
    implicit def c =new X[Ex1] { def id =println("Ex1") }
    implicit def d =new X[Base] { def id =println("Base") }
  def f[T](a :T)(implicit g :X[T]) = g.id
  
  f(5)
  f('c')
  f(x)
  */
  //5 times println("now")
    
    //val start = System.nanoTime
    //Thread sleep 1000
    //val end = System.nanoTime
    //(start - end)/1E9
    
}
