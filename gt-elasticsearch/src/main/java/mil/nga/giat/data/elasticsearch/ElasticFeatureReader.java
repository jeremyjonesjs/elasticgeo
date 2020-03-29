/*
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import mil.nga.giat.data.elasticsearch.ElasticDataStore.ArrayEncoding;
import mil.nga.giat.shaded.es.common.joda.Joda;
import mil.nga.giat.shaded.joda.time.format.DateTimeFormatter;

import static mil.nga.giat.data.elasticsearch.ElasticConstants.DATE_FORMAT;
import static mil.nga.giat.data.elasticsearch.ElasticConstants.FULL_NAME;

import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * FeatureReader access to the Elasticsearch index.
 */
class ElasticFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final static Logger LOGGER = Logging.getLogger(ElasticFeatureReader.class);

    private final ContentState state;

    private final SimpleFeatureType featureType;

    private final float maxScore;

    private final ObjectMapper mapper;

    private final ArrayEncoding arrayEncoding;

    private SimpleFeatureBuilder builder;

    private Iterator<ElasticHit> searchHitIterator;

    private Iterator aggregationIterator;

    private final ElasticParserUtil parserUtil;

    private boolean combineBucketsIntoSingleFeature;

    private int maxDocCount;

    private static final float SCALED_MAX_DOC_COUNT = 1000f;

    public ElasticFeatureReader(ContentState contentState, ElasticResponse response) {
        this(contentState, response, false);
    }

    public ElasticFeatureReader(ContentState contentState, ElasticResponse response, boolean combineBucketsIntoSingleFeature) {
        this(contentState, response.getHits(), response.getAggregations(), response.getMaxScore(), combineBucketsIntoSingleFeature);
    }

    public ElasticFeatureReader(ContentState contentState, List<ElasticHit> hits, Map<String,ElasticAggregation> aggregations, float maxScore) {
        this(contentState, hits, aggregations, maxScore, false);
    }

    public ElasticFeatureReader(ContentState contentState, List<ElasticHit> hits, Map<String,ElasticAggregation> aggregations, float maxScore, boolean combineBucketsIntoSingleFeature) {
        this.state = contentState;
        this.featureType = state.getFeatureType();
        this.searchHitIterator = hits.iterator();
        this.builder = new SimpleFeatureBuilder(featureType);
        this.parserUtil = new ElasticParserUtil();
        this.maxScore = maxScore;
        this.combineBucketsIntoSingleFeature = combineBucketsIntoSingleFeature;

        this.aggregationIterator = Collections.emptyIterator();
        if (aggregations != null && !aggregations.isEmpty()) {
            String aggregationName = aggregations.keySet().stream().findFirst().orElse(null);
            if (aggregations.size() > 1) {
                LOGGER.info("Result has multiple aggregations. Using " + aggregationName);
            }

            if (combineBucketsIntoSingleFeature) {
                List<Map<String, Object>> buckets = aggregations.get(aggregationName).getBuckets();
                if (buckets != null) {
                    // TODO JEJ changed to return a single entry with all the buckets for performance
                    this.aggregationIterator = Arrays.asList(buckets).iterator();
                }
            } else {
                if (aggregations.get(aggregationName).getBuckets() != null) {
                    List<Map<String, Object>> buckets = aggregations.get(aggregationName).getBuckets();
                    Collections.sort(buckets, (Comparator<Map<String, Object>>) (o1, o2) -> {
                        return ((Number) o1.get("doc_count")).intValue() - ((Number) o2.get("doc_count")).intValue();
                    });
                    this.aggregationIterator = buckets.iterator();
                }

                maxDocCount = 0;
                for (Iterator iter = aggregations.get(aggregationName).getBuckets().iterator(); iter.hasNext();) {
                    Map<String, Object> aggregation = (Map<String, Object>) iter.next();
                    if (aggregation.containsKey("doc_count")) {
                        maxDocCount = Math.max(maxDocCount, ((Number) aggregation.get("doc_count")).intValue());
                    }
                }
                LOGGER.severe("MAX DOC COUNT: " + maxDocCount);
            }
        }

        if (contentState.getEntry() != null && contentState.getEntry().getDataStore() != null) {
            final ElasticDataStore dataStore;
            dataStore = (ElasticDataStore) contentState.getEntry().getDataStore();
            this.arrayEncoding = dataStore.getArrayEncoding();
        } else {
            this.arrayEncoding = ArrayEncoding.valueOf((String) ElasticDataStoreFactory.ARRAY_ENCODING.getDefaultValue());
        }

        this.mapper = new ObjectMapper();
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return this.featureType;
    }

    @Override
    public SimpleFeature next() {
        final String id;
        if (searchHitIterator.hasNext()) {
            id = nextHit();
        } else {
            id = nextAggregation();
        }
        return builder.buildFeature(id);
    }

    private String nextHit() {
        final ElasticHit hit = searchHitIterator.next();
        final SimpleFeatureType type = getFeatureType();
        final Map<String, Object> source = hit.getSource();

        final Float score;
        final Float relativeScore;
        if (hit.getScore() != null && !Float.isNaN(hit.getScore()) && maxScore>0) {
            score = hit.getScore();
            relativeScore = score / maxScore;
        } else {
            score = null;
            relativeScore = null;
        }

        for (final AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
            final String name = descriptor.getType().getName().getLocalPart();
            final String sourceName = (String) descriptor.getUserData().get(FULL_NAME);

            List<Object> values = hit.field(sourceName);
            if (values == null && source != null) {
                // read field from source
                values = parserUtil.readField(source, sourceName);
            }

            if (values == null && sourceName.equals("_id")) {
                builder.set(name, hit.getId());
            } else if (values == null && sourceName.equals("_index")) {
                builder.set(name, hit.getIndex());
            } else if (values == null && sourceName.equals("_type")) {
                builder.set(name, hit.getType());
            } else if (values == null && sourceName.equals("_score")) {
                builder.set(name, score);
            } else if (values == null && sourceName.equals("_relative_score")) {
                builder.set(name, relativeScore);
            } else if (values != null && Geometry.class.isAssignableFrom(descriptor.getType().getBinding())) {
                if (values.size() == 1) {
                    builder.set(name, parserUtil.createGeometry(values.get(0)));
                } else {
                    builder.set(name, parserUtil.createGeometry(values));
                }
            } else if (values != null && Date.class.isAssignableFrom(descriptor.getType().getBinding())) {
                Object dataVal = values.get(0);
                if (dataVal instanceof Double) {
                    builder.set(name, new Date(Math.round((Double) dataVal)));
                } else if (dataVal instanceof Integer) {
                    builder.set(name, new Date((Integer) dataVal));
                } else if (dataVal instanceof Long) {
                    builder.set(name, new Date((long) dataVal));
                } else {
                    final String format = (String) descriptor.getUserData().get(DATE_FORMAT);
                    final DateTimeFormatter dateFormatter = Joda.forPattern(format).parser();

                    Date date = dateFormatter.parseDateTime((String) dataVal).toDate();
                    builder.set(name, date);
                }
            } else if (values != null && values.size() == 1) {
                builder.set(name, values.get(0));
            } else if (values != null && !name.equals("_aggregation")) {
                final Object value;
                if (arrayEncoding == ArrayEncoding.CSV) {
                    // only include first array element when using CSV array encoding
                    value = values.get(0);
                } else {
                    value = values;
                }
                builder.set(name, value);
            }
        }

        return state.getEntry().getTypeName() + "." + hit.getId();
    }

    private String nextAggregation() {
        if (combineBucketsIntoSingleFeature) {
            builder.set("_aggregation", aggregationIterator.next());
            return null;
        } else {
            final Map<String, Object> aggregation = (Map<String, Object>) aggregationIterator.next();
            String id = (String) aggregation.get("key");
            int docCount = ((Number) aggregation.get("doc_count")).intValue();
            int scaledDocCount = scaleDocCount(docCount);

            builder.set("_id", id);
            builder.set("_index", "cell-towers");
            builder.set("_relative_score", 1f);
            builder.set("_score", 1f);
            builder.set("_type", "_doc");
            builder.set("range", scaledDocCount);

            LatLong latLong = GeoHash.decodeHash(id);
            Map<String, Object> location = new LinkedHashMap<>();
            location.put("lat", latLong.getLat());
            location.put("lon", latLong.getLon());
            builder.set("location", parserUtil.createGeometry(location));

            return id;
        }
    }

    private int scaleDocCount(int docCount) {
        if (docCount == 0) { return 0; }

        return (int) Math.round((SCALED_MAX_DOC_COUNT / Math.log(maxDocCount)) * Math.log(docCount));
    }

    @Override
    public boolean hasNext() {
        return searchHitIterator.hasNext() || aggregationIterator.hasNext();
    }

    @Override
    public void close() {
        builder = null;
        searchHitIterator = null;
    }

}
