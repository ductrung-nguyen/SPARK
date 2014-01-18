package test

object test2 {

    def abs(x: Double) = if (x < 0) -x else x     //> abs: (x: Double)Double

    def sqrt(x: Double) = {
        def sqrtIter(guess: Double): Double =
            if (isGoodEnough(guess)) guess
            else sqrtIter(improveGuess(guess))

        def isGoodEnough(guess: Double) =
            if (abs(guess * guess - x) / x <= 0.001) true else false

        def improveGuess(guess: Double) =
            (x / guess + guess) / 2

        sqrtIter(1.0)
    }                                             //> sqrt: (x: Double)Double

    sqrt(1e60)                                    //> res0: Double = 1.0000788456669446E30
    
    def factorial(n : Int): Int = {
    	def loop(acc: Int, n:Int) : Int =
    		if (n == 0) acc
    		else loop(acc*n, n-1)
    	loop(1,n)
    }                                             //> factorial: (n: Int)Int
    
    factorial(3)                                  //> res1: Int = 6
    
    
    
    

}