package auxillary;

public class Percent {
    public static int percent(int part, int whole){
        
        return 0;
    }
    public static long percent(long part, long whole){
        return Math.round( getPercent( (double)part, (double)whole ) );
    }
    private static double getPercent(double part, double whole){
        return part * 100 / whole;
    }
}
