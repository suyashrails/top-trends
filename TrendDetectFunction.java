/**
 * Detects bursts in engagement for each trend token using rolling statistics.
 *
 * Input:  PostEvent   (token, platform, postCount, likes, comments, timestamp)
 * Output: TrendDetection (token, platform, windowEnd, zScore, engagement)
 */
public class TrendDetectFunction
        extends KeyedProcessFunction<String, PostEvent, TrendDetection> {

    // Flink keyed state: rolling stats for this token (mean, stddev, count)
    private transient ValueState<Stats> rollingStats;

    @Override
    public void open(Configuration cfg) {
        ValueStateDescriptor<Stats> statsDesc =
            new ValueStateDescriptor<>("stats", Stats.class);
        rollingStats = getRuntimeContext().getState(statsDesc);
    }

    /**
     * Called for every input event.
     *
     * @param evt  Incoming post event (see below)
     * @param ctx  Flink context (timestamp, key, timer access)
     * @param out  Output collector for TrendDetection events
     */
    @Override
    public void processElement(
            PostEvent evt,         // input: single social post or aggregate
            Context ctx,           // context: key, time, etc.
            Collector<TrendDetection> out   // output: detected bursts
    ) throws Exception {
        // 1. Load or initialize rolling stats for this token
        Stats stats = rollingStats.value();
        if (stats == null) stats = Stats.init();

        // 2. Compute engagement score for the event
        double engagement = evt.getPostCount() + 0.2 * evt.getLikes() + 0.5 * evt.getComments();

        // 3. Compute z-score vs historical mean/stddev for this trend token
        double z = stats.std() > 0 ? (engagement - stats.mean()) / stats.std() : 0.0;

        // 4. Emit detection if burst threshold exceeded
        if (z > 2.5 && engagement > 1000) {
            out.collect(new TrendDetection(
                evt.getToken(),
                evt.getPlatform(),
                ctx.timestamp(),   // marks window end
                z,
                engagement
            ));
        }

        // 5. Update rolling stats for the next window
        Stats updated = stats.update(engagement);
        rollingStats.update(updated);
    }
}
