package test

object test2 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(75); 

    def abs(x: Double) = if (x < 0) -x else x;System.out.println("""abs: (x: Double)Double""");$skip(385); 

    def sqrt(x: Double) = {
        def sqrtIter(guess: Double): Double =
            if (isGoodEnough(guess)) guess
            else sqrtIter(improveGuess(guess))

        def isGoodEnough(guess: Double) =
            if (abs(guess * guess - x) / x <= 0.001) true else false

        def improveGuess(guess: Double) =
            (x / guess + guess) / 2

        sqrtIter(1.0)
    };System.out.println("""sqrt: (x: Double)Double""");$skip(16); val res$0 = 

    sqrt(1e60);System.out.println("""res0: Double = """ + $show(res$0));$skip(151); 
    
    def factorial(n : Int): Int = {
    	def loop(acc: Int, n:Int) : Int =
    		if (n == 0) acc
    		else loop(acc*n, n-1)
    	loop(1,n)
    };System.out.println("""factorial: (n: Int)Int""");$skip(22); val res$1 = 
    
    factorial(3);System.out.println("""res1: Int = """ + $show(res$1))}
    
    
    
    

}