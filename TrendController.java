@RestController
@RequestMapping("/trends")
@RequiredArgsConstructor
public class TrendController {

    private final RestHighLevelClient esClient; // ES Java blocking client

    // --- Endpoint: get summary for a trend ---
    @GetMapping("/{id}/summary")
    public TrendSummary getTrendSummary(@PathVariable String id) throws IOException {
        GetRequest getReq = new GetRequest("trend_search_v1", id);
        GetResponse getResp = esClient.get(getReq, RequestOptions.DEFAULT);
        if (!getResp.isExists()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Trend not found: " + id);
        }
        return JsonUtil.fromJson(getResp.getSourceAsString(), TrendSummary.class);
    }

    // --- Endpoint: get related trends using embedding similarity ---
    @GetMapping("/{id}/related")
    public List<TrendSummary> getRelatedTrends(
            @PathVariable String id,
            @RequestParam(defaultValue = "5") int k) throws IOException {

        // 1. Fetch the embedding for the given trend
        GetRequest getReq = new GetRequest("trend_search_v1", id);
        GetResponse getResp = esClient.get(getReq, RequestOptions.DEFAULT);
        if (!getResp.isExists()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Trend not found: " + id);
        }
        TrendSummary mainTrend = JsonUtil.fromJson(getResp.getSourceAsString(), TrendSummary.class);
        float[] embedding = mainTrend.getEmbedding(); // e.g., [0.1f, -0.3f, ...]

        // 2. Build a k-NN query using XContentBuilder
        XContentBuilder knnQuery = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("knn")
                    .field("field", "embedding")
                    .field("query_vector", embedding)
                    .field("k", k + 1) // Fetch k+1 to exclude self later
                .endObject()
            .endObject();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.wrapperQuery(knnQuery.string()))
            .size(k + 1);

        SearchRequest searchRequest = new SearchRequest("trend_search_v1")
            .source(sourceBuilder);

        // 3. Execute search and filter out the original trend
        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
        List<TrendSummary> results = Arrays.stream(searchResponse.getHits().getHits())
            .map(hit -> JsonUtil.fromJson(hit.getSourceAsString(), TrendSummary.class))
            .filter(t -> !t.getTrendId().equals(id))
            .limit(k)
            .collect(Collectors.toList());

        return results;
    }
}
