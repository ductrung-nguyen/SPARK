package rtreelib

object Utility {
	 /**
     * Parse a string to double
     */
    def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }
    
}