# Sample Elasticsearch Query Application
Sample template for querying elasticsearch data, for further downstream processing

## Getting Started

### Prerequisites
You will need running Elasticsearch instances. To set them up, see:
* Elasticsearch Quickstart: https://www.elastic.co/downloads/elasticsearch

### Using the Template
* Main "high-level" dataflow can be scripted into `ElasticsearchDataflow` under `getTasks` method, where new query tasks are defined (based on each elasticsearch index patterns)
* Configurations (e.g. index patterns, es instances, etc) can be made into `application.properties` file. Environment specific configurations to be overwritten can be supplied into other `application-[env].properties` file, and passed to the jvm when starting it, by supplying `--spring.profiles.active=[env]` argument when starting the java jar file. E.g. `java -jar /path/to/.jar --spring.profiles.active=dev` will pick up properties from `application-dev.properties` file as well
* Implement your downstream data processing logic under `ingestSearchHit` method in `ElasticsearchScrollTask`
* For more information, see: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high.html

### Writing Test Cases
* WIP

### Building the Application
* To build the application, cd to project home and execute `mvn clean install`

## Built With
* [Elasticsearch](https://www.elastic.co/products/elasticsearch) - Distributed, RESTful search engine
* [Spring Boot](https://spring.io/projects/spring-boot) - Opinionated Framework for Spring Based Applications for Dependency Injections
* [Java](https://adoptopenjdk.net/) - OpenJDK from AdoptOpenJDK
* [Maven](https://maven.apache.org/) - Dependency Management
* [Lombok](https://projectlombok.org/download) - Framework for Reducing Boilerplate Codes
