package com.yongyct.templates.elasticsearchquerytemplate.dataflow;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import com.yongyct.templates.elasticsearchquerytemplate.tasks.ElasticsearchScrollTask;
import com.yongyct.templates.elasticsearchquerytemplate.tasks.ElasticsearchTaskState;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * Main dataflow for the elasticsearch query ingestion programme
 * 
 * @author tommy.yong
 *
 */
@Component
@Slf4j
public class ElasticsearchDataflow {

	/**
	 * Configuration values for elasticsearch client connection
	 */
	@Value("${es.maxConnectionAttempts}")
	private int maxConnectionAttempts;
	@Value("${es.connectionRetryBackoffMs}")
	private int connectionRetryBackoffMs;
	@Value("${es.indices}")
	private String[] indices;
	// To change incrementField(s)'s datatype to String[] if different indices have
	// different increment fields
	@Value("${es.incrementField}")
	private String incrementField;
	@Value("${es.scrollSize}")
	private int scrollSize;
	@Value("${es.scrollKeepaliveMin}")
	private int scrollKeepaliveMin;
	@Value("${es.pollInterval}")
	private int pollInterval;

	@Autowired
	private RestHighLevelClient esClient;

	@Setter
	private ApplicationContext applicationContext;

	private ScheduledExecutorService executorService;

	/**
	 * Multithreads various query tasks, with each query task belonging to a certain
	 * index pattern
	 */
	@PostConstruct
	public void runDataflow() {

		List<Runnable> tasks = getTasks();
		int numTasks = tasks.size();
		if (numTasks > 0) {
			executorService = Executors.newScheduledThreadPool(numTasks);
		} else {
			log.warn("No tasks scheduled, exiting program");
			closeApplicationContext();
		}
		for (int i = 0; i < numTasks; i++) {
			executorService.scheduleAtFixedRate(tasks.get(i), i * pollInterval / numTasks, pollInterval,
					TimeUnit.MILLISECONDS);
		}

	}

	/**
	 * Pre-destroy cleanup of esClient and executorService
	 * 
	 * @throws IOException
	 */
	@PreDestroy
	public void shutdown() throws IOException {
		esClient.close();
		executorService.shutdown();
	}

	/**
	 * Provides the list of query tasks to be executed
	 * 
	 * @return {@link List<Runnable>} list of elasticsearch query tasks
	 */
	private List<Runnable> getTasks() {
		// TODO Add more tasks based on number of indices patterns/increment fields
		// supplied

		ElasticsearchTaskState myIndexTaskState = new ElasticsearchTaskState(null, 0);

		Runnable myIndexElasticsearchTask = new ElasticsearchScrollTask("Index Search Task", indices[0], incrementField,
				esClient, scrollKeepaliveMin, scrollSize, maxConnectionAttempts, connectionRetryBackoffMs,
				myIndexTaskState);

		return Arrays.asList(myIndexElasticsearchTask);
	}

	/**
	 * Close applicationContext / shuts down the jvm & spring boot application
	 */
	private void closeApplicationContext() {
		ConfigurableApplicationContext ctx = (ConfigurableApplicationContext) applicationContext;
		ctx.close();
	}

}
