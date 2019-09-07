package com.yongyct.templates.elasticsearchquerytemplate.tasks;

import java.io.IOException;
import java.util.Arrays;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * Class is encapsulates each search task based on the index pattern to be
 * searched
 * 
 * @author tommy.yong
 *
 */
@Getter
@Setter
@AllArgsConstructor
@Slf4j
public class ElasticsearchScrollTask implements Runnable {

	/**
	 * Task specific properties
	 */
	private String name;
	private String index;
	private String incrementField;

	/**
	 * Common properties/objects across tasks
	 */
	private RestHighLevelClient esClient;
	private int scrollKeepaliveMin;
	private int scrollSize;
	private int maxConnectionAttempts;
	private int connectionRetryBackoffMs;

	/**
	 * Storing of Task State TODO: Persist task state for recovery
	 */
	private ElasticsearchTaskState esTaskState;

	@Override
	public void run() {
		executeScroll();
	}

	/**
	 * Main flow for executing scroll query for the index pattern
	 */
	private void executeScroll() {

		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(scrollKeepaliveMin));
		SearchRequest searchRequest = prepareInitialRequest(scroll);
		SearchResponse searchResponse = new SearchResponse();

		for (int i = 0; i < maxConnectionAttempts; i++) {

			try {
				executeSearchOperations(searchRequest, searchResponse, scroll);
				break;
			} catch (IOException e) {
				log.error("IOException", e);
			}

			try {
				log.warn("Connection failed, backing off for {} ms before retrying", connectionRetryBackoffMs);
				Thread.sleep(connectionRetryBackoffMs);
			} catch (InterruptedException e) {
				log.error("Connection backoff interrupted", e);
			}

		}

	}

	/**
	 * Method prepares the initial query request for the scroll query
	 * 
	 * @param scroll
	 * @return {@link SearchRequest} initial search request to be used for further
	 *         client querying
	 */
	private SearchRequest prepareInitialRequest(Scroll scroll) {

		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(index);
		searchRequest.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED);
		searchRequest.scroll(scroll);

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		QueryBuilder queryBuilder;

		if (esTaskState.getLastIncrementFieldValue() == null) {
			queryBuilder = QueryBuilders.matchAllQuery();
		} else {
			queryBuilder = QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery(incrementField).gt(esTaskState.getLastIncrementFieldValue()));
		}

		searchSourceBuilder.query(queryBuilder).sort(incrementField, SortOrder.ASC);
		searchSourceBuilder.size(scrollSize);

		return searchRequest.source(searchSourceBuilder);
	}

	/**
	 * This methods execute the scroll search operations
	 * 
	 * @param searchRequest  provided by prepareInitialRequest
	 * @param searchResponse to capture searchResponse from esClient searchRequest
	 * @param scroll
	 * @throws IOException
	 */
	private void executeSearchOperations(SearchRequest searchRequest, SearchResponse searchResponse, Scroll scroll)
			throws IOException {

		searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);

		// Handle failure for failed shards
		if (searchResponse.getFailedShards() > 0) {
			Arrays.asList(searchResponse.getShardFailures()).stream()
					.forEach(shard -> log.warn("Failure in shard: {}", shard.toString()));
			log.error("Failure querying shard(s)");
		}

		String scrollId = searchResponse.getScrollId();
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		processHits(searchHits);

		while (searchHits != null && searchHits.length > 0) {
			SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
			searchScrollRequest.scroll(scroll);
			searchResponse = esClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
			scrollId = searchResponse.getScrollId();
			searchHits = searchResponse.getHits().getHits();
			processHits(searchHits);
		}

		try {
			if (scrollId != null) {
				clearScroll(scrollId);
			} else {
				log.warn("No scrollContext/scrollId is retrieved, index pattern supplied is "
						+ "probably not matching what is available in the elasticsearch server");
			}
		} catch (ElasticsearchStatusException e) {
			log.error("Clearing of scroll request failed", e);
		}

	}

	/**
	 * This method processes the searchHits when they are returned in each
	 * searchResponse
	 * 
	 * @param searchHits
	 */
	private void processHits(SearchHit[] searchHits) {

		for (SearchHit searchHit : searchHits) {

			ingestSearchHit(searchHit);

			String searchHitIncrementFieldValue = searchHit.getSourceAsMap().get(incrementField).toString();
			String lastIncrementFieldValue = esTaskState.getLastIncrementFieldValue();
			if (lastIncrementFieldValue == null
					|| (searchHitIncrementFieldValue.compareTo(lastIncrementFieldValue) > 0)) {
				esTaskState.setLastIncrementFieldValue(searchHitIncrementFieldValue);
			}
			esTaskState.incrementRecordsCount();
		}

	}

	/**
	 * This method clears the scroll request when it is complete
	 * 
	 * @param scrollId
	 */
	private void clearScroll(String scrollId) {

		ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
		clearScrollRequest.addScrollId(scrollId);
		ClearScrollResponse clearScrollResponse = null;

		for (int i = 0; i < maxConnectionAttempts; i++) {
			try {
				clearScrollResponse = esClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
				if (clearScrollResponse.isSucceeded()) {
					break;
				}
			} catch (IOException e) {
				log.error("IOException during clearing of scroll request", e);
			}
		}

		if (!clearScrollResponse.isSucceeded()) {
			throw new ElasticsearchException("Clearing of scroll request faield");
		}

	}

	/**
	 * Use by processHits method. TODO: Implement treatment of data after querying
	 * from elasticsearch here
	 * 
	 * @param searchHit to be used for processing (e.g. to feed to downstream system
	 *                  or perform business logic)
	 */
	private void ingestSearchHit(SearchHit searchHit) {
		log.info("Ingested data: {}" + searchHit.getSourceAsString());
	}

}
