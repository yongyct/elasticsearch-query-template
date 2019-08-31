package com.yongyct.templates.elasticsearchquerytemplate.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * Class contains configuration for connecting to an elasticsearch cluster/node
 * Class also provides elasticsearch client bean for main dataflow of
 * application to use for performing elasticsearch client operations e.g.
 * querying
 * 
 * @author tommy.yong
 *
 */
@Configuration
@Slf4j
public class ElasticsearchConnector {

	/**
	 * Basic configuration values for connecting to elasticsearch server
	 */
	@Value("${es.instances}")
	private String[] instances;
	@Value("${es.connectionScheme}")
	private String connectionScheme;
	@Value("${es.username}")
	private String username;
	@Value("${es.password}")
	private String password;

	/**
	 * Creates client object for performing elasticsearch client operations
	 * 
	 * @return {@link RestHighLevelClient} for performing client operations e.g.
	 *         querying
	 */
	@Bean
	public RestHighLevelClient esClient() {

		List<HttpHost> esNodes = new ArrayList<>();

		for (String instance : instances) {
			String host = instance.split(":")[0];
			int port = Integer.parseInt(instance.split(":")[1]);
			HttpHost httpHost = new HttpHost(host, port, connectionScheme);
			esNodes.add(httpHost);
		}

		RestClientBuilder restClientBuilder = RestClient.builder(esNodes.toArray(new HttpHost[0]));

		if (!username.equals("") && !username.equals("")) {
			Credentials credentials = new UsernamePasswordCredentials(username, password);
			CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY, credentials);
			restClientBuilder.setHttpClientConfigCallback(
					httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
		} else {
			log.warn("No/incomplete credentials provided, reverting to default of no authentication");
		}

		restClientBuilder.setFailureListener(new RestClient.FailureListener() {
			@Override
			public void onFailure(Node node) {
				log.error("Error connecting to node: {}", node.toString());
			}
		});

		restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

		return new RestHighLevelClient(restClientBuilder);

	}

}
