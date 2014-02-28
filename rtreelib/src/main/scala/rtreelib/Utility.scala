package rtreelib

object Utility {
    /**
     * Parse a string to double
     */
    def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }

    def normalizeString(s: String) = {
        var s1 = s.trim
        val len = s1.length
        if (len <= 2 || s1 == "\"\"") s1
        else if (s1(0) == '\"' && s1(len - 1) == '\"')
            s1.substring(1, len - 1)
        else s1
    }

}