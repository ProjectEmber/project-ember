# Project Ember 1.0.0

The source code for the City of Light

For a complete paper (wrote according the ACM standard) about the project and its assumption, visit [here](./documentation/acm_ember.pdf) and drop us a line! To access slides made to demo the project, visit [here](./documentation/slides_ember.pdf)

## About
This is an academic project born to be an efficient solution for the CINI 2017 Challenge on smart cities illumination systems. In particular, the goal was to prototype and test a solution which was capable of (near) real-time data stream processing for monitoring records from street lamps, lumen sensors co-located with the street lamps themselves and from traffic data produced by third-party APIs. We will explore this solution for the following use case: in a smart city context it is necessary to guarantee the maximum efficiency from lamps consumption while providing an optimal illumination within safety limits for pedestrians and drivers according to local traffic intensity. To achieve that, it is necessary to project a grid of smart lamps capable of tuning their light level according to the right amount of energy necessary to provide city aware, safe and green consumption levels. This grid must be powered and managed via a reliable, highly available, processing-capable control system. Introducing Project Ember.

## Usage
Project Ember is thought to be usable out-of-the-box. To install Apache Flink on your development machine or to configure an HA JobManager with multiple TaskManagers, you can follow the official guide at [flink.apache.org](https://flink.apache.org). 
You can also give a try to a very simple `docker-compose.yml` under the `dockerutils`folder. Once the framework is online, visit 
`<jobmanageraddress>:8081` to load your jar and monitor your cluster status.

## Configuration
Project Ember is customizable according the environment it will be run upon. You can specify a path to a `.properties` file to override the default configuration. To use it you have only to launch the program, inside the Flink directory, with the command `bin/flink run project-ember.jar config.properties`. 

The following table shows some configurations options:

|             property             | type    | description                                                                                                                                  | default        |
|--------------------------------- |---------|----------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| lifespan.rank.min                | long    | Lifespan ranking window - slide from (min, max)                                                                                              | 1              |
| lifespan.rank.max                | long    | Lifespan ranking window - slide from (min, max)                                                                                              | 60             |
| lifespan.rank.size               | integer | Lifespan ranking results, max number of items                                                                                                | 10             |
| lifespan.days.max                | integer | Lifespan of a single bulb, security limit (in days)                                                                                          | 200            |
| alerts.period.seconds            | long    | Interval (in seconds) to perform a query on Elasticsearch to detect any anomaly                                                              | 30             |
| alerts.electricalfailure.seconds | long    | Expiration time (in seconds) to consider an electrical failure for a not responding lamp                                                     | 90             |
| elasticsearch.cluster.name       | string  | Elasticsearch cluster name (security option enabled by default)                                                                              | "embercluster" |
| elasticsearch.cluster.address    | string  | Elasticsearch cluster remote address                                                                                                         | "localhost"    |
| elasticsearch.cluster.port       | int     | Elasticsearch cluster port opened (by default 9200 for web-based queries and 9300 for clients)                                               | 9300           |
| kafka.cluster.address            | string  | Apache Kafka cluster remote address                                                                                                          | "localhost"    |
| kafka.cluster.port               | int     | Apache Kafka cluster port opened                                                                                                             | 9092           |
| kafka.topic.alert                | boolean | Option to enable the alerts messages production under topic 'alert', else alerts are stored by lamp ID in Elasticsearch under 'alert' index. | true           |

Disclaimer: this project is in its early release, and it is made to be available to a large panorama of institutions. If you want a particular configuration, please contact us or open an issue to ask for new features.

## Control Parameters
Please, for a custom experience or to change the behavior of the control operator for particular environments (as for example different regulations or parking silos), feel free to change the parameters
in [EmberControlRoom.java](./src/main/java/it/uniroma2/ember/operators/join/EmberControlRoom.java) before building and deploying your solution.
