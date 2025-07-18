import java.util.List;

public class TrendSummary {
    private String trendId;
    private String displayName;
    private String canonicalName;
    private String category;
    private double currentPopularity;
    private double popularityDelta;
    private int rank;
    private String topPlatform;
    private String lastUpdated;
    private float[] embedding; // For semantic similarity
    private List<PlatformMetrics> platformMetrics; // Optional, for more detail

    public TrendSummary() {}

    // ...getters and setters...
}
