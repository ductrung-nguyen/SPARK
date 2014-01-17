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
}

  val x = new Poly(Map(1 -> 2.0, 3 -> 4.0, 5 -> 6.2))
                                                  //> x  : learnscala1.test11.Poly = 6.2x^5 + 4.0x^3 + 2.0x^1
  val y = new Poly(0 -> 3.0, 3 -> 7.0)            //> y  : learnscala1.test11.Poly = 7.0x^3 + 3.0x^0
  val z = new Poly(true)                          //> z  : learnscala1.test11.Poly = 3.0x^3 + 2.0x^2 + 1.0x^1
  x + z                                           //> res0: learnscala1.test11.Poly = 6.2x^5 + 7.0x^3 + 2.0x^2 + 3.0x^1
  x + y                                           //> res1: learnscala1.test11.Poly = 6.2x^5 + 11.0x^3 + 2.0x^1 + 3.0x^0
  
  val map = Map(1 -> 2.0, 2-> 3.0, 3->4.0, 4-> 6.0)
                                                  //> map  : scala.collection.immutable.Map[Int,Double] = Map(1 -> 2.0, 2 -> 3.0, 
                                                  //| 3 -> 4.0, 4 -> 6.0)
  val map2 = Map(2->8.0, 3-> 1.0)                 //> map2  : scala.collection.immutable.Map[Int,Double] = Map(2 -> 8.0, 3 -> 1.0)
                                                  //| 
}