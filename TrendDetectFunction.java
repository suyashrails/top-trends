/**
 * Detects bursts in engagement for each n-gram (bigram/trigram/hashtag) using rolling statistics.
 *
 * Input:  PostEvent   (token = n-gram string, platform, postCount, likes, comments, timestamp)
 * Output: TrendDetection (token, platform, windowEnd, zScore, engagement)
 */
public class TrendDetectFunction
        extends KeyedProcessFunction<String, PostEvent, TrendDetection> {

    // State: rolling mean & stddev for engagement per n-gram (token)
    private transient ValueState<Stats> rollingStats;

    @Override
    public void open(Configuration cfg) {
        ValueStateDescriptor<Stats> statsDesc =
            new ValueStateDescriptor<>("stats", Stats.class);
        rollingStats = getRuntimeContext().getState(statsDesc);
    }

    /**
     * For each PostEvent (one per n-gram), update state and emit trend if burst.
     *
     * @param evt  PostEvent with n-gram token, engagement info
     * @param ctx  Flink context (key = n-gram)
     * @param out  Output collector
     */
    @Override
    public void processElement(
            PostEvent evt,
            Context ctx,
            Collector<TrendDetection> out
    ) throws Exception {
        // Key for this state is now the n-gram (bigram/trigram/hashtag)
        Stats stats = rollingStats.value();
        if (stats == null) stats = Stats.init();

        // Compute engagement for this n-gram in this post
        double engagement = evt.getPostCount() + 0.2 * evt.getLikes() + 0.5 * evt.getComments();

        // Calculate burstiness (z-score) for this n-gram
        double z = stats.std() > 0 ? (engagement - stats.mean()) / stats.std() : 0.0;

        // If n-gram's engagement is a burst, emit a detected trend
        if (z > 2.5 && engagement > 1000) {
            out.collect(new TrendDetection(
                evt.getToken(),      // n-gram phrase or hashtag
                evt.getPlatform(),
                ctx.timestamp(),
                z,
                engagement
            ));
        }

        // Update rolling stats for this n-gram
        Stats updated = stats.update(engagement);
        rollingStats.update(updated);
    }
}
