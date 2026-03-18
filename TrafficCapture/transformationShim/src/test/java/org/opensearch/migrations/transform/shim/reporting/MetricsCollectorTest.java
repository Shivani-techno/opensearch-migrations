package org.opensearch.migrations.transform.shim.reporting;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.migrations.transform.shim.validation.TargetResponse;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MetricsCollectorTest {

    /** Simple in-memory sink for testing. */
    static class CapturingSink implements MetricsSink {
        final List<ValidationDocument> documents = new ArrayList<>();
        @Override public void submit(ValidationDocument document) { documents.add(document); }
        @Override public void flush() {}
        @Override public void close() {}
    }

    private Map<String, Object> solrResponseBody(long numFound, int qTime) {
        return Map.of(
            "response", Map.of("numFound", numFound),
            "responseHeader", Map.of("QTime", qTime)
        );
    }

    private Map<String, Object> requestMap(String method, String uri) {
        return Map.of("method", method, "URI", uri, "headers", Map.of("Host", "localhost"));
    }

    private TargetResponse successResponse(String name, Map<String, Object> parsedBody) {
        return new TargetResponse(name, 200, new byte[0], parsedBody,
            Duration.ofMillis(10), Duration.ZERO, Duration.ZERO, null);
    }

    private Map<String, Map<String, Object>> emptyPerTargetMetrics() {
        return new LinkedHashMap<>();
    }

    @Test
    void collectExtractsHitCountsAndLatency() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        var responses = Map.of(
            "solr", successResponse("solr", solrResponseBody(100, 12)),
            "opensearch", successResponse("opensearch", solrResponseBody(95, 15))
        );
        collector.collect(requestMap("GET", "/solr/mycore/select"), null, responses, emptyPerTargetMetrics());

        assertEquals(1, sink.documents.size());
        var doc = sink.documents.get(0);
        assertEquals(100L, doc.solrHitCount());
        assertEquals(95L, doc.opensearchHitCount());
        assertEquals(5.0, doc.hitCountDriftPercentage());
        assertEquals(12L, doc.solrQtimeMs());
        assertEquals(15L, doc.opensearchTookMs());
        assertEquals(3L, doc.queryTimeDeltaMs());
        assertEquals("mycore", doc.collectionName());
        assertEquals("/solr/{collection}/select", doc.normalizedEndpoint());
    }

    @Test
    void collectPopulatesOriginalRequest() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        var responses = Map.of("solr", successResponse("solr", solrResponseBody(10, 1)));
        collector.collect(requestMap("GET", "/solr/core1/select?q=*:*"), null, responses, emptyPerTargetMetrics());

        var doc = sink.documents.get(0);
        assertNotNull(doc.originalRequest());
        assertEquals("GET", doc.originalRequest().method());
        assertEquals("/solr/core1/select?q=*:*", doc.originalRequest().uri());
        assertNull(doc.originalRequest().body());
    }

    @Test
    void requestBodyIncludedWhenFlagTrue() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, true);
        var reqMap = Map.<String, Object>of("method", "POST", "URI", "/solr/c/update",
            "headers", Map.of(), "payload", "{\"add\":{}}");
        var responses = Map.of("solr", successResponse("solr", Map.of()));
        collector.collect(reqMap, null, responses, emptyPerTargetMetrics());

        assertNotNull(sink.documents.get(0).originalRequest().body());
        assertEquals("{\"add\":{}}", sink.documents.get(0).originalRequest().body());
    }

    @Test
    void requestBodyExcludedWhenFlagFalse() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        var reqMap = Map.<String, Object>of("method", "POST", "URI", "/solr/c/update",
            "headers", Map.of(), "payload", "{\"add\":{}}");
        var responses = Map.of("solr", successResponse("solr", Map.of()));
        collector.collect(reqMap, null, responses, emptyPerTargetMetrics());

        assertNull(sink.documents.get(0).originalRequest().body());
    }

    @Test
    void nullResponsesProduceNullFields() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        collector.collect(requestMap("GET", "/solr/c/select"), null, Map.of(), emptyPerTargetMetrics());

        var doc = sink.documents.get(0);
        assertNull(doc.solrHitCount());
        assertNull(doc.opensearchHitCount());
        assertNull(doc.hitCountDriftPercentage());
    }

    @Test
    void customMetricsEmptyWhenNoneEmitted() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        var responses = Map.of("solr", successResponse("solr", Map.of()));
        collector.collect(requestMap("GET", "/solr/c/select"), null, responses, null);

        assertTrue(sink.documents.get(0).customMetrics().isEmpty());
    }

    @Test
    void customMetricsMergedFromMultipleTargets() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        var responses = Map.of("solr", successResponse("solr", Map.of()));
        var perTargetMetrics = new LinkedHashMap<String, Map<String, Object>>();
        perTargetMetrics.put("solr", Map.of("solr-metric", 1));
        perTargetMetrics.put("opensearch", Map.of("os-metric", 2));
        collector.collect(requestMap("GET", "/solr/c/select"), null, responses, perTargetMetrics);

        var doc = sink.documents.get(0);
        assertEquals(1, doc.customMetrics().get("solr-metric"));
        assertEquals(2, doc.customMetrics().get("os-metric"));
    }

    @Test
    void exceptionInCollectDoesNotPropagate() {
        var throwingSink = new MetricsSink() {
            @Override public void submit(ValidationDocument d) { throw new RuntimeException("boom"); }
            @Override public void flush() {}
            @Override public void close() {}
        };
        var collector = new MetricsCollector(throwingSink, false);
        assertDoesNotThrow(() ->
            collector.collect(requestMap("GET", "/solr/c/select"), null, Map.of(), emptyPerTargetMetrics()));
    }

    @Test
    void guardSkipsWhenResponseCountIsNotTwo() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        // Only 1 response — should skip
        var responses = Map.of("solr", successResponse("solr", solrResponseBody(10, 1)));
        var transformedReqs = Map.of("opensearch", Map.<String, Object>of("method", "GET", "URI", "/os/_search"));
        collector.collect(requestMap("GET", "/solr/c/select"), transformedReqs, responses, emptyPerTargetMetrics());
        assertTrue(sink.documents.isEmpty());
    }

    @Test
    void guardSkipsWhenTransformedRequestCountIsNotOne() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        var responses = Map.of(
            "solr", successResponse("solr", solrResponseBody(10, 1)),
            "opensearch", successResponse("opensearch", solrResponseBody(10, 1))
        );
        // No transformed requests — should skip
        collector.collect(requestMap("GET", "/solr/c/select"), emptyPerTargetMetrics(), responses, emptyPerTargetMetrics());
        assertTrue(sink.documents.isEmpty());
    }

    @Test
    void resolvedOverloadWorksDirectly() {
        var sink = new CapturingSink();
        var collector = new MetricsCollector(sink, false);
        var original = new ValidationDocument.RequestRecord("GET", "/solr/c/select", Map.of(), null);
        var transformed = new ValidationDocument.RequestRecord("GET", "/c/_search", Map.of(), null);
        var primary = successResponse("solr", solrResponseBody(100, 12));
        var secondary = successResponse("opensearch", solrResponseBody(95, 15));
        collector.collect(original, transformed, primary, secondary, Map.of("warn", 1));

        assertEquals(1, sink.documents.size());
        var doc = sink.documents.get(0);
        assertEquals(100L, doc.solrHitCount());
        assertEquals(95L, doc.opensearchHitCount());
        assertNotNull(doc.transformedRequest());
        assertEquals("/c/_search", doc.transformedRequest().uri());
        assertEquals(1, doc.customMetrics().get("warn"));
    }
}
