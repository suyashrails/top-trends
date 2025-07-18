// Rolling stats helper
public class Stats {
    // mean, std, count...
    public static Stats init() { return new Stats(0, 1, 0); }
    public double mean() { ... }
    public double std()  { ... }
    public Stats update(double x) { ... }
}
