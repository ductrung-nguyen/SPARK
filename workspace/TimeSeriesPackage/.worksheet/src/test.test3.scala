package test

object test3 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(144); 
    def product(f: Int => Int)(a: Int, b: Int): Int =
        if (a > b) 1
        else f(a) * product(f)(a + 1, b);System.out.println("""product: (f: Int => Int)(a: Int, b: Int)Int""");$skip(32); val res$0 = 

    product(x => x * x)(3, 4);System.out.println("""res0: Int = """ + $show(res$0));$skip(51); ;

    def factorial(n: Int) = product(x => x)(1, n);System.out.println("""factorial: (n: Int)Int""");$skip(18); val res$1 = 

    factorial(3);System.out.println("""res1: Int = """ + $show(res$1));$skip(47); 

    def abs(x: Double) = if (x < 0) -x else x;System.out.println("""abs: (x: Double)Double""");$skip(100); 

    def isCloseEnough(a: Double, b: Double) =
        if (abs(a - b) / b < 0.0001) true else false;System.out.println("""isCloseEnough: (a: Double, b: Double)Boolean""");$skip(284); 

    def fixedPoint(f: Double => Double)(firstPoint: Double) = {
        def iter(guess: Double) :Double = {
            val nextGuess = f(guess)
            if (isCloseEnough(guess, nextGuess)) guess
            else iter(nextGuess)
        }
        
        iter(firstPoint)
    };System.out.println("""fixedPoint: (f: Double => Double)(firstPoint: Double)Double""");$skip(72); 
    
    def averageDamp(f: Double => Double)(x: Double) = (x + f(x))/2;System.out.println("""averageDamp: (f: Double => Double)(x: Double)Double""");$skip(63); 
		
		def sqrt(x:Double) = fixedPoint(averageDamp(y => x/y))(1);System.out.println("""sqrt: (x: Double)Double""");$skip(13); val res$2 = 
		
		sqrt(4);System.out.println("""res2: Double = """ + $show(res$2))}
}