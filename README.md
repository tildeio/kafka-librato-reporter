# Kafka Librato Reporter

Reports Kafka metrics to Librato.

# Usage

* Add jar to Kafka's lib directory
* Add the following to Kafka's server.properties

```
kafka.metrics.reporters=io.tilde.kafka.metrics.KafkaLibratoReporter

# Configure reporting metrics to Librato
librato.username=[username]
librato.token=[api-token]
librato.agent.identifier=[hostname]
```
