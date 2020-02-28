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
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Envelope;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.opengis.filter.spatial.BBOX;
import org.opengis.geometry.BoundingBox;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AggregationCacheImpl implements AggregationCache {

    private final static Logger LOGGER = Logging.getLogger(AggregationCacheImpl.class);

    private RTree<Map<String, Object>, Geometry> tree;

    public AggregationCacheImpl() {
        tree = RTree.star().maxChildren(6).create();
    }

    @Override
    public void initialize(ElasticDataStore dataStore) throws IOException {
        ElasticRequest searchRequest = prepareSearchRequest(null);
        ElasticResponse response = dataStore.getClient().search(dataStore.getIndexName(), "cell-towers", searchRequest);
        List<Map<String, Object>> buckets = response.getAggregations().values().iterator().next().getBuckets();
        LOGGER.severe("*** INITIALIZED WITH " + buckets.size() + " BUCKETS");
        this.putBuckets(null, buckets);
    }

    @Override
    public List<Map<String, Object>> getBuckets(Query query) {
        BoundingBox bounds = ((BBOX) query.getFilter()).getBounds();

        LOGGER.severe("... ORIGINAL bounds: [" + bounds.getMinX() + "," + bounds.getMinY() + "," + bounds.getMaxX() + "," + bounds.getMaxY());

        Rectangle rectangle = Geometries.rectangleGeographic(bounds.getMinX(), bounds.getMinY(), bounds.getMaxX(), bounds.getMaxY());
        if (Math.abs(rectangle.x2() - rectangle.x1()) < 1d || rectangle.x2() - 360 - rectangle.x1() < 1d) {
            rectangle = Geometries.rectangle(-180, rectangle.y1(), 180, rectangle.y2());
        }

        LOGGER.severe("... Searching cache for bounds: [" + rectangle.x1() + "," + rectangle.y1() + "," + rectangle.x2() + "," + rectangle.y2());

        Iterable<Entry<Map<String, Object>, Geometry>> results =
                tree.search(rectangle);

        List<Map<String, Object>> buckets = StreamSupport.stream(results.spliterator(), false)
                .map(entry -> entry.value())
                .collect(Collectors.toList());

        LOGGER.severe("... Found " + buckets.size() + " buckets");

        return buckets;
    }

    @Override
    public void putBuckets(Query query, List<Map<String, Object>> buckets) {
        List<Entry<Map<String, Object>, Geometry>> entries = new ArrayList<>();
        for (Map<String, Object> bucket : buckets) {
            LatLong latLong = GeoHash.decodeHash((String) bucket.get("key"));
            entries.add(EntryDefault.entry(bucket, Geometries.pointGeographic(latLong.getLon(), latLong.getLat())));
        }
        LOGGER.severe("... Adding " + entries.size() + " buckets to cache");
        tree = tree.add(entries);
    }

    private ElasticRequest prepareSearchRequest(Query query) throws IOException {
        final ElasticRequest searchRequest = new ElasticRequest();

        //searchRequest.setQuery(queryBuilder);

        Map<String, Object> grid = new HashMap<>();
        grid.put("field", "location");
        grid.put("precision", "4");
        grid.put("size", "100000");
        Map<String, Map<String, Object>> aggregation = new HashMap<>();
        aggregation.put("geohash_grid", grid);
        Map<String, Map<String, Map<String, Object>>> aggregations = new HashMap<>();
        aggregations.put("agg", aggregation);

        searchRequest.setAggregations(aggregations);
        searchRequest.setSize(0);

        return searchRequest;
    }

}
