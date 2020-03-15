package mil.nga.giat.data.elasticsearch;

import org.geotools.data.Query;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface AggregationCache {

    void initialize(ElasticDataStore dataStore) throws IOException;

    List<Map<String, Object>> getBuckets(int precision, Query query);

    boolean supportsQuery(FilterToElastic filterToElastic);

}
