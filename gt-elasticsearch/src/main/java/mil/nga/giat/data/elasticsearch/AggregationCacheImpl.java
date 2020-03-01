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

    private Map<Integer, RTree<Map<String, Object>, Geometry>> treesByPrecision;

    public AggregationCacheImpl() {
        treesByPrecision = new HashMap<>();
    }

    @Override
    public void initialize(ElasticDataStore dataStore) throws IOException {
        performAggregationAndCacheResults(dataStore, 4, 90);
        performAggregationAndCacheResults(dataStore, 5, 30);
        performAggregationAndCacheResults(dataStore, 6, 10);
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

    private void putBuckets(int precision, List<Map<String, Object>> buckets) {
        LOGGER.severe("... Creating tree with " + buckets.size() + " buckets for precision " + precision);
        treesByPrecision.put(precision, RTree.star().minChildren(30).maxChildren(100).create(buckets.stream().map(bucket -> {
            final LatLong latLong = GeoHash.decodeHash((String) bucket.get("key"));
            return EntryDefault.entry(bucket, (Geometry) Geometries.pointGeographic(latLong.getLon(), latLong.getLat()));
        }).collect(Collectors.toList())));
    }

    private void performAggregationAndCacheResults(ElasticDataStore dataStore, int precision, int latitudeSize) throws IOException {
        List<Map<String, Object>> buckets = new ArrayList<>();
        for (double minLat = -180d; minLat <= (180d - latitudeSize); minLat += latitudeSize) {
            buckets.addAll(performAggregation(dataStore, precision, minLat, minLat + latitudeSize));
        }
        LOGGER.severe("*** INITIALIZED PRECISION " + precision + " WITH " + buckets.size() + " BUCKETS");
        this.putBuckets(precision, buckets);
    }

    private List<Map<String, Object>> performAggregation(ElasticDataStore dataStore, int precision, double minLat, double maxLat) throws IOException {
        ElasticRequest searchRequest = prepareSearchRequest(precision, minLat, maxLat);
        ElasticResponse response = dataStore.getClient().search(dataStore.getIndexName(), "cell-towers", searchRequest);
        return response.getAggregations().values().iterator().next().getBuckets();
    }

    private ElasticRequest prepareSearchRequest(int precision, double minLat, double maxLat) {
        final ElasticRequest searchRequest = new ElasticRequest();

        Map<String, Object> must = new HashMap<>();
        Map<String, Object> matchAll = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        must.put("match_all", matchAll);

        Map<String, Object> location = new HashMap<>();
        Map<String, Object> geoBoundingBox = new HashMap<>();
        Map<String, Object> filter = new HashMap<>();
        location.put("bottom_left", Arrays.asList(minLat, -90));
        location.put("top_right", Arrays.asList(maxLat, 90));
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
        Map<String, Map<String, Map<String, Object>>> aggregations = new HashMap<>();
        aggregations.put("agg", aggregation);

        searchRequest.setAggregations(aggregations);
        searchRequest.setSize(0);

        return searchRequest;
    }

}
