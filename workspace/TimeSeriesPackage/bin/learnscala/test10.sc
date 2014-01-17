package learnscala1

object test10 {

	def insert(x: Int, xs: List[Int]) : List[Int] = xs match {
  	case List() => x::Nil
  	case y::ys => if (x < y) x::xs else y::insert(x, ys)
  }                                               //> insert: (x: Int, xs: List[Int])List[Int]
  
	def sort(xs:List[Int]) : List[Int] = xs match {
		case List() => List()
		case y::ys => insert(y, sort(ys))
	}                                         //> sort: (xs: List[Int])List[Int]
	
	sort(List(4,2,9,6,8))                     //> res0: List[Int] = List(2, 4, 6, 8, 9)
	
	
	def msort[T](xs: List[T])(implicit ord: Ordering[T] ) : List[T] = xs match {
		case List() => List()
		case x:: Nil => xs
		case x:: xs1 => {
			val n = xs.length/2
			def merge(x1 : List[T], x2: List[T]):List[T] = (x1, x2) match {
				case (x1, Nil) => x1
				case (Nil, x2) => x2
				case (x1head::x1tail, x2head::x2tail) =>
					if (ord.lt(x1head , x2head)) x1head::merge(x1tail, x2)
					else x2head::merge(x1,x2tail)
			}
			val (left,right) = xs.splitAt(n)
			merge(msort(left), msort(right))
		}
	}                                         //> msort: [T](xs: List[T])(implicit ord: Ordering[T])List[T]
	
	val l = List(4, 6, 2,-3 ,5,0,8,9)         //> l  : List[Int] = List(4, 6, 2, -3, 5, 0, 8, 9)
	msort(l)(Ordering.Int)                    //> res1: List[Int] = List(-3, 0, 2, 4, 5, 6, 8, 9)
	val fruits = List("Apple", "Orange", "CoCo", "Banana")
                                                  //> fruits  : List[java.lang.String] = List(Apple, Orange, CoCo, Banana)
	msort(fruits)                             //> res2: List[java.lang.String] = List(Apple, Banana, CoCo, Orange)
	
	l filter (x => x > 0)                     //> res3: List[Int] = List(4, 6, 2, 5, 8, 9)
	l filterNot (x => x > 0)                  //> res4: List[Int] = List(-3, 0)
	l partition (x => x > 0)                  //> res5: (List[Int], List[Int]) = (List(4, 6, 2, 5, 8, 9),List(-3, 0))
	l takeWhile (x => x > 2)                  //> res6: List[Int] = List(4, 6)
	l dropWhile (x => x > 2)                  //> res7: List[Int] = List(2, -3, 5, 0, 8, 9)
	l span (x => x > 2)                       //> res8: (List[Int], List[Int]) = (List(4, 6),List(2, -3, 5, 0, 8, 9))
	
	def pack[T] (xs : List[T]) : List[List[T]] = xs match {
		case Nil => Nil
		case x :: xs1 => {
			val (l, r) = xs span (y => y == x)
			l :: pack[T](r)
		}
	}                                         //> pack: [T](xs: List[T])List[List[T]]
	
	def encode[T] (xs : List[T]) :  List[Pair[T,Int]] = {
		val p = pack(xs)
		p map (x => (x.head, x.length))
		
	}                                         //> encode: [T](xs: List[T])List[(T, Int)]
	
	pack(List("a", "a", "a", "b", "b", "c", "d", "d"))
                                                  //> res9: List[List[java.lang.String]] = List(List(a, a, a), List(b, b), List(c
                                                  //| ), List(d, d))
  encode(List("a", "a", "a", "b", "b", "c", "d", "d"))
                                                  //> res10: List[(java.lang.String, Int)] = List((a,3), (b,2), (c,1), (d,2))
  encode(List())                                  //> res11: List[(Nothing, Int)] = List()
  
  
  def sum(xs : List[Int]) : Int = 0 :: xs reduceLeft (_ + _)
                                                  //> sum: (xs: List[Int])Int
  def sum1(xs : List[Int]) : Int = (xs foldLeft 0) ( _ + _)
                                                  //> sum1: (xs: List[Int])Int
  def product(xs : List[Int]) : Int = (xs foldLeft 1) (_ * _)
                                                  //> product: (xs: List[Int])Int
          
  sum(l)                                          //> res12: Int = 31
  sum1(l)                                         //> res13: Int = 31
  product(l)                                      //> res14: Int = 0
	
  val s = "Hello World"                           //> s  : java.lang.String = Hello World
  s flatMap (c => "." + c)                        //> res15: String = .H.e.l.l.o. .W.o.r.l.d
  s flatMap (c => List('.', c))                   //> res16: String = .H.e.l.l.o. .W.o.r.l.d
  val x = s flatMap (c => Array(1,2))             //> x  : scala.collection.immutable.IndexedSeq[Int] = Vector(1, 2, 1, 2, 1, 2, 
                                                  //| 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2)
  x.length                                        //> res17: Int = 22
  
  def isPrime(n: Int) = (((2 to n/2) filter ( x => n % x == 0)).length == 0)
                                                  //> isPrime: (n: Int)Boolean
  isPrime(1)                                      //> res18: Boolean = true
  
  
  def findSumPrime(n: Integer) = {
  val result : List[(Int, Int)] = Nil
  
  	(1 until n) flatMap (
  		i => (1 until i) map (
  			j => (i,j)
  		)
  	) filter (pair => isPrime(pair._1 + pair._2))
  	
  }                                               //> findSumPrime: (n: Integer)scala.collection.immutable.IndexedSeq[(Int, Int)]
                                                  //| 
  def findSumPrime2(n: Integer) =
  	for {
  		i <- 1 until n	 // generator
  		j <- 1 until i	// generator
  		if (isPrime(i + j))
  	} yield (i,j)                             //> findSumPrime2: (n: Integer)scala.collection.immutable.IndexedSeq[(Int, Int)
                                                  //| ]
  
  findSumPrime(5)                                 //> res19: scala.collection.immutable.IndexedSeq[(Int, Int)] = Vector((2,1), (3
                                                  //| ,2), (4,1), (4,3))
  findSumPrime2(5)                                //> res20: scala.collection.immutable.IndexedSeq[(Int, Int)] = Vector((2,1), (3
                                                  //| ,2), (4,1), (4,3))
  
  
  def scalaProduct(xs: List[Double], ys: List[Double]) =
  	(for {
  		x <- xs zip ys
  	} yield (x._1 * x._2)) sum                //> scalaProduct: (xs: List[Double], ys: List[Double])Double
  	
  scalaProduct(List(1,2,3), List(4,5,6))          //> res21: Double = 32.0
  
  
  def queens(n : Int) : Set[List[Int]] = {
  	def putQueen(k : Int) : Set[List[Int]] =
  		if (k == 0) Set(List())
  		else
  			for {
  				queens <- putQueen(k - 1)
  				col <- 0 until n
  				if (isSafe(col, queens))
  			} yield col :: queens
  	
  	
  			
  	def isSafe(col : Int, queens : List[Int]) : Boolean = {
  		val numrow = queens.length
  		val pastSolution = ( numrow -1 to 0 by -1) zip queens
  		pastSolution forall {
  			case (r, c) => c != col && math.abs(col - c) != numrow - r
  		}
  	}
  	
  	putQueen(n)
  }                                               //> queens: (n: Int)Set[List[Int]]
  
  def showQueens(solution: List[Int]) = {
  	val lines =
  	for ( col <- solution)
  		yield (Vector.fill(solution.length)("* ").updated(col, "X ")) mkString
  	
  	"\n" + (lines mkString "\n")
  }                                               //> showQueens: (solution: List[Int])java.lang.String
  
  (queens(4) map showQueens ) mkString "\n"       //> res22: String = "
                                                  //| * X * * 
                                                  //| * * * X 
                                                  //| X * * * 
                                                  //| * * X * 
                                                  //| 
                                                  //| * * X * 
                                                  //| X * * * 
                                                  //| * * * X 
                                                  //| * X * * "
  
}
	
	