package test

object test4 {
  var x = new Rational(3,9)                       //> x  : test.Rational = 1/3
  var y = new Rational(5,7)                       //> y  : test.Rational = 5/7
  var z = new Rational(3,2)                       //> z  : test.Rational = 3/2
  x.add(y)                                        //> res0: test.Rational = 22/21
  x.neg()                                         //> res1: test.Rational = 1/-3
  x.subtract(y).subtract(z)                       //> res2: test.Rational = -79/42
  x.less(y)                                       //> res3: Boolean = true
  x.maximum(y)                                    //> res4: test.Rational = 5/7
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