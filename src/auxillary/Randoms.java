package auxillary;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zmr
 */
public class Randoms {
    private final static Logger LOG = Logger.getLogger(Randoms.class.getName());
    /**
     * Get random int between two positive numbers. Uses Math.Random, the double is cast to int, decimal part truncated
     * @param start
     * @param end
     * @return 
     * @throws IllegalArgumentException if first value is less than second value or one of them is negative
     */
    public static int getRandInRange(int start, int end){
        if(end<start||start<0 || end<0){
            throw new IllegalArgumentException("Range start greater than range end or one of them negative");
        }
        int range = end - start;
        int result = getRandToVal(range);
        result += start;
        return result;
    }
    /**
     * Get random int from 0 to specified value. Uses Math.Random, the double is cast to int, decimal part truncated
     * @param maxValue
     * @return 
     */
    public static int getRandToVal(int maxValue){
        Double seed = Math.random();
        Double result = maxValue * seed;
        //LOG.log(Level.INFO, "Rand value: {0}", result);
        return result.intValue();
    }
    /**
     * Get random ASCII number. Numbers and letters 48-122. Uses Math.Random, the double is cast to int, decimal part truncated
     * @param start
     * @param end
     * @return 
     * @throws IllegalArgumentException if first value is less than second value or one of them is negative or they are outside of ASCII value range.
     */
    public static String getRandChar(int start, int end){
        if(start<0||start>127 || end<start||end>127){
            throw new IllegalArgumentException("Range values outside of ASCII or start is greater than end");
        }
        char c = (char)getRandInRange(start, end);        
        return Character.toString( c );
    }
}
