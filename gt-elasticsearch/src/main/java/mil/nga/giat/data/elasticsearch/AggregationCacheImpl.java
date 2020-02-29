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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        performAggregationAndCacheResults(dataStore, 4);
        performAggregationAndCacheResults(dataStore, 5);
    }

    @Override
    public List<Map<String, Object>> getBuckets(int precision, Query query) {
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

        return buckets;
    }

    private void putBuckets(int precision, List<Map<String, Object>> buckets) {
        List<Entry<Map<String, Object>, Geometry>> entries = new ArrayList<>();
        for (Map<String, Object> bucket : buckets) {
            LatLong latLong = GeoHash.decodeHash((String) bucket.get("key"));
            entries.add(EntryDefault.entry(bucket, Geometries.pointGeographic(latLong.getLon(), latLong.getLat())));
        }
        LOGGER.severe("... Creating tree with " + entries.size() + " buckets for precision " + precision);
        treesByPrecision.put(precision, RTree.create(entries)); // TODO decide on best rtree config
    }

    private void performAggregationAndCacheResults(ElasticDataStore dataStore, int precision) throws IOException {
        List<Map<String, Object>> buckets = performAggregation(dataStore, precision);
        LOGGER.severe("*** INITIALIZED PRECISION " + precision + " WITH " + buckets.size() + " BUCKETS");
        this.putBuckets(precision, buckets);
    }

    private List<Map<String, Object>> performAggregation(ElasticDataStore dataStore, int precision) throws IOException {
        ElasticRequest searchRequest = prepareSearchRequest(precision);
        ElasticResponse response = dataStore.getClient().search(dataStore.getIndexName(), "cell-towers", searchRequest);
        return response.getAggregations().values().iterator().next().getBuckets();
    }

    private ElasticRequest prepareSearchRequest(int precision) throws IOException {
        final ElasticRequest searchRequest = new ElasticRequest();

        //searchRequest.setQuery(queryBuilder);

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
