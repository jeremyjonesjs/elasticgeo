package mil.nga.giat.data.elasticsearch;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import com.github.davidmoten.rtree2.Entry;
import com.github.davidmoten.rtree2.RTree;
import com.github.davidmoten.rtree2.geometry.Geometries;
import com.github.davidmoten.rtree2.geometry.Geometry;
import com.github.davidmoten.rtree2.geometry.Rectangle;
import com.github.davidmoten.rtree2.internal.EntryDefault;
import org.geotools.data.Query;
import org.geotools.util.logging.Logging;
import org.opengis.filter.spatial.BBOX;
import org.opengis.geometry.BoundingBox;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AggregationCacheImpl implements AggregationCache {

    private final static Logger LOGGER = Logging.getLogger(AggregationCacheImpl.class);

    private static final int MAX_CACHED_PRECISION = 6;
    private static final int MAX_NESTED_AGGREGATION_PRECISION = 5;

    private Map<Integer, RTree<Map<String, Object>, Geometry>> treesByPrecision;

    public AggregationCacheImpl() {
        treesByPrecision = new HashMap<>();
    }

    @Override
    public void initialize(ElasticDataStore dataStore) throws IOException {
        performAggregationAndCacheResults(dataStore, 4, 90, 180);
        performAggregationAndCacheResults(dataStore, 5, 10, 90);
        performAggregationAndCacheResults(dataStore, 6, 10, 90);
    }

    @Override
    public List<Map<String, Object>> getBuckets(int precision, Query query) {
        long start = System.currentTimeMillis();

        BoundingBox bounds = ((BBOX) query.getFilter()).getBounds();

        LOGGER.severe("... ORIGINAL bounds: [" + bounds.getMinX() + "," + bounds.getMinY() + "," + bounds.getMaxX() + "," + bounds.getMaxY());

        Rectangle rectangle = Geometries.rectangleGeographic(bounds.getMinX(), bounds.getMinY(), bounds.getMaxX(), bounds.getMaxY());
        if (Math.abs(rectangle.x2() - rectangle.x1()) < 1d || rectangle.x2() - 360 - rectangle.x1() < 1d) {
            rectangle = Geometries.rectangle(-180, rectangle.y1(), 180, rectangle.y2());
        }

        LOGGER.severe("... Searching cache for bounds: [" + rectangle.x1() + "," + rectangle.y1() + "," + rectangle.x2() + "," + rectangle.y2());

        Iterable<Entry<Map<String, Object>, Geometry>> results =
                treesByPrecision.get(precision).search(rectangle);

        List<Map<String, Object>> buckets = StreamSupport.stream(results.spliterator(), false)
                .map(entry -> entry.value())
                .collect(Collectors.toList());

        LOGGER.severe("... Found " + buckets.size() + " buckets for precision " + precision);

        LOGGER.severe("RTree search took " + (System.currentTimeMillis() - start) + " ms");

        return buckets;
    }

    @Override
    public boolean supportsQuery(FilterToElastic filterToElastic) {
        boolean nestedAggregationRequested = false;
        Integer precision = null;
        if (filterToElastic.getAggregations() != null && filterToElastic.getAggregations().containsKey("agg")) {
            precision = Integer.parseInt((String) filterToElastic.getAggregations().get("agg").get("geohash_grid").get("precision"));
            nestedAggregationRequested = filterToElastic.getAggregations().get("agg").size() > 1;
        }

        boolean userFilterApplied = filterToElastic.getNativeQueryBuilder().size() != 1 || filterToElastic.getNativeQueryBuilder().keySet().iterator().next() != "match_all";

        return precision != null &&
                precision <= MAX_CACHED_PRECISION &&
                !userFilterApplied &&
                (!nestedAggregationRequested || precision <= MAX_NESTED_AGGREGATION_PRECISION);
    }

    private void putBuckets(int precision, List<Map<String, Object>> buckets) {
        LOGGER.severe("... Creating tree with " + buckets.size() + " buckets for precision " + precision);
        treesByPrecision.put(precision, RTree.star().minChildren(30).maxChildren(100).create(buckets.stream().map(bucket -> {
            final LatLong latLong = GeoHash.decodeHash((String) bucket.get("key"));
            return EntryDefault.entry(bucket, (Geometry) Geometries.pointGeographic(latLong.getLon(), latLong.getLat()));
        }).collect(Collectors.toList())));
    }

    private void performAggregationAndCacheResults(ElasticDataStore dataStore, int precision, int latitudeSize, int longitudeSize) throws IOException {
        List<Map<String, Object>> buckets = new ArrayList<>();
        for (double minLat = -180d; minLat <= (180d - latitudeSize); minLat += latitudeSize) {
            for (double minLon = -90d; minLon <= (90 - longitudeSize); minLon += longitudeSize) {
                LOGGER.severe("Agg for precision " + precision + " lat: " + minLat + " to " + (minLat + latitudeSize) + ", lon: " + minLon + " to " + (minLon + longitudeSize));
                buckets.addAll(performAggregation(dataStore, precision, minLat, minLat + latitudeSize, minLon, minLon + longitudeSize));
            }
        }
        LOGGER.severe("Initialized precision " + precision + " with " + buckets.size() + " buckets");
        this.putBuckets(precision, buckets);
    }

    private List<Map<String, Object>> performAggregation(ElasticDataStore dataStore, int precision, double minLat, double maxLat, double minLon, double maxLon) throws IOException {
        ElasticRequest searchRequest = prepareSearchRequest(precision, minLat, maxLat, minLon, maxLon);
        ElasticResponse response = dataStore.getClient().search(dataStore.getIndexName(), "cell-towers", searchRequest);
        return response.getAggregations().values().iterator().next().getBuckets();
    }

    private ElasticRequest prepareSearchRequest(int precision, double minLat, double maxLat, double minLon, double maxLon) {
        final ElasticRequest searchRequest = new ElasticRequest();

        Map<String, Object> must = new HashMap<>();
        Map<String, Object> matchAll = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        must.put("match_all", matchAll);

        Map<String, Object> location = new HashMap<>();
        Map<String, Object> geoBoundingBox = new HashMap<>();
        Map<String, Object> filter = new HashMap<>();
        location.put("bottom_left", Arrays.asList(minLat, minLon));
        location.put("top_right", Arrays.asList(maxLat, maxLon));
        geoBoundingBox.put("location", location);
        filter.put("geo_bounding_box", geoBoundingBox);

        Map<String, Object> bool = new HashMap<>();
        bool.put("must", must);
        bool.put("filter", filter);
        query.put("bool", bool);

        searchRequest.setQuery(query);

        Map<String, Object> grid = new HashMap<>();
        grid.put("field", "location");
        grid.put("precision", String.valueOf(precision));
        grid.put("size", "1000000");
        Map<String, Map<String, Object>> aggregation = new HashMap<>();
        aggregation.put("geohash_grid", grid);

        if (precision <= MAX_NESTED_AGGREGATION_PRECISION) {
            Map<String, Object> nested = new HashMap<>();
            nested.put("mcc", createTermsAggregation("mcc"));
            aggregation.put("aggs", nested);
        }

        Map<String, Map<String, Map<String, Object>>> aggregations = new HashMap<>();
        aggregations.put("agg", aggregation);

        searchRequest.setAggregations(aggregations);
        searchRequest.setSize(0);

        return searchRequest;
    }

    private Map<String, Map<String, Object>> createTermsAggregation(String fieldName) {
        Map<String, Map<String, Object>> aggregation = new HashMap<>();

        Map<String, Object> terms = new HashMap<>();
        terms.put("field", fieldName);

        aggregation.put("terms", terms);

        return aggregation;
    }

}
