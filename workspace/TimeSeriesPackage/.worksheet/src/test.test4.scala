package test

object test4 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(56); 
  var x = new Rational(3,9);System.out.println("""x  : test.Rational = """ + $show(x ));$skip(28); 
  var y = new Rational(5,7);System.out.println("""y  : test.Rational = """ + $show(y ));$skip(28); 
  var z = new Rational(3,2);System.out.println("""z  : test.Rational = """ + $show(z ));$skip(11); val res$0 = 
  x.add(y);System.out.println("""res0: test.Rational = """ + $show(res$0));$skip(10); val res$1 = 
  x.neg();System.out.println("""res1: test.Rational = """ + $show(res$1));$skip(28); val res$2 = 
  x.subtract(y).subtract(z);System.out.println("""res2: test.Rational = """ + $show(res$2));$skip(12); val res$3 = 
  x.less(y);System.out.println("""res3: Boolean = """ + $show(res$3));$skip(15); val res$4 = 
  x.maximum(y);System.out.println("""res4: test.Rational = """ + $show(res$4))}
}

class Rational(x: Int, y : Int) {
	
	require(y != 0, "Denominator must be non-zero")
	
	private def gcd(a: Int, b: Int) : Int =
		if (b == 0) a else gcd(b, a % b)
		
	private val g = gcd(x,y)
	var numer = x/g
	var denom = y/g
	
	
	
	def add(that : Rational) : Rational =
		new Rational(numer*that.denom + denom*that.numer, denom*that.denom)
		
	def neg() =
		new Rational(-numer, denom)
		
	def subtract(that: Rational) =
		new Rational(numer*that.denom - denom*that.numer, denom*that.denom)
		
	def less(that: Rational) = numer * that.denom < denom * that.numer
	
	def maximum(that: Rational) =
		if (less(that)) that else this
		
	override def toString() =
			numer + "/" + denom
}