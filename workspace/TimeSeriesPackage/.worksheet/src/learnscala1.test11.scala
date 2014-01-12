package learnscala1

object test11 {

class Poly(val terms0: Map[Int, Double]){
	val terms = terms0 withDefaultValue 0.0
	def this(binds : (Int, Double)*) = this(binds.toMap)
	def this(bool : Boolean) = this (Map(1 -> 1.0, 2 -> 2.0,3 -> 3.0))
	
	def + (that : Poly) = new Poly( (that.terms foldLeft terms)(addTerm))
	
	def addTerm(terms : Map[Int, Double], term : (Int, Double)) : Map[Int, Double] = {
		val (exp, cof) = term
		terms + (exp -> (cof + terms(exp)) )
	}

	override def toString() =
		(for ((exp, coff) <- terms.toList.sorted.reverse) yield coff + "x^" + exp) mkString " + "
};import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(644); 

  val x = new Poly(Map(1 -> 2.0, 3 -> 4.0, 5 -> 6.2));System.out.println("""x  : learnscala1.test11.Poly = """ + $show(x ));$skip(39); 
  val y = new Poly(0 -> 3.0, 3 -> 7.0);System.out.println("""y  : learnscala1.test11.Poly = """ + $show(y ));$skip(25); 
  val z = new Poly(true);System.out.println("""z  : learnscala1.test11.Poly = """ + $show(z ));$skip(8); val res$0 = 
  x + z;System.out.println("""res0: learnscala1.test11.Poly = """ + $show(res$0));$skip(8); val res$1 = 
  x + y;System.out.println("""res1: learnscala1.test11.Poly = """ + $show(res$1));$skip(55); 
  
  val map = Map(1 -> 2.0, 2-> 3.0, 3->4.0, 4-> 6.0);System.out.println("""map  : scala.collection.immutable.Map[Int,Double] = """ + $show(map ));$skip(34); 
  val map2 = Map(2->8.0, 3-> 1.0);System.out.println("""map2  : scala.collection.immutable.Map[Int,Double] = """ + $show(map2 ))}
}