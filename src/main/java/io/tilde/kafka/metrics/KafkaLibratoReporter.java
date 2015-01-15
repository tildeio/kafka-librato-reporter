package io.tilde.kafka.metrics;

import com.yammer.metrics.core.*;
import java.io.*;

import com.librato.metrics.LibratoReporter;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.librato.metrics.LibratoReporter.ExpandedMetric;
import static com.librato.metrics.LibratoReporter.ExpandedMetric.*;
import static com.librato.metrics.LibratoReporter.MetricExpansionConfig;

@SuppressWarnings("unused")
public class KafkaLibratoReporter
  implements KafkaMetricsReporter, KafkaLibratoReporterMBean {

  private final Logger LOG = LoggerFactory.getLogger(KafkaLibratoReporter.class);

  private LibratoReporter.Builder libratoReporterBuilder;

  private LibratoReporter libratoReporter;

  @Override
  public synchronized void init(VerifiableProperties props) {
    libratoReporterBuilder = LibratoReporter.builder(
      props.getString("librato.username"),
      props.getString("librato.token"),
      props.getString("librato.agent.identifier"))
    ;


    Set<ExpandedMetric> metrics = new HashSet<ExpandedMetric>();
    maybeEnableMetric(props, metrics, MEDIAN, true);
    maybeEnableMetric(props, metrics, PCT_75, false);
    maybeEnableMetric(props, metrics, PCT_95, true);
    maybeEnableMetric(props, metrics, PCT_98, false);
    maybeEnableMetric(props, metrics, PCT_99, false);
    maybeEnableMetric(props, metrics, PCT_999, true);

    maybeEnableMetric(props, metrics, COUNT, false);
    maybeEnableMetric(props, metrics, RATE_MEAN, false);
    maybeEnableMetric(props, metrics, RATE_1_MINUTE, true);
    maybeEnableMetric(props, metrics, RATE_5_MINUTE, false);
    maybeEnableMetric(props, metrics, RATE_15_MINUTE, false);

    final List<String> metricsWhiteList = Arrays.asList(props.getString("librato.kafka.metrics.whitelist", ".*").split("\\s*,\\s*"));
    LOG.info("metricsWhiteList: " + metricsWhiteList);

    libratoReporterBuilder
      .setTimeout(props.getInt("librato.timeout", 20), TimeUnit.SECONDS)
      .setReportVmMetrics(false)
      .setExpansionConfig(new MetricExpansionConfig(metrics))
      .setPredicate(new MetricPredicate() {
        @Override
        public boolean matches(final MetricName name, final Metric metric) {
          for (String s : metricsWhiteList) {
            if (name.getName().matches(s)) {
              LOG.debug("matched! pattern: " + s + " metric name: " + name.getName());
              return true;
            }
          }
          LOG.debug(name.getName() + " found no matches on whitelist");
          return false;
        }
      })
    ;

    if (props.getBoolean("librato.kafka.enable", true)) {
      startReporter(props.getInt("librato.kafka.interval", 30));
    }
  }

  private static void maybeEnableMetric(
    VerifiableProperties props,
    Set<ExpandedMetric> metrics,
    ExpandedMetric metric,
    boolean defaultValue) {

    if (props.getBoolean(metric.buildMetricName("librato.kafka.metrics"), defaultValue)) {
      metrics.add(metric);
    }
  }

  @Override
  public String getMBeanName() {
    return "kafka:type=" + KafkaLibratoReporter.class.getCanonicalName();
  }

  @Override
  public synchronized void startReporter(long interval) {
    if (libratoReporterBuilder == null) {
      throw new IllegalStateException("reporter not configured");
    }

    if (libratoReporter == null) {
      libratoReporter = libratoReporterBuilder.build();
      libratoReporter.start(interval, TimeUnit.SECONDS);
    }
  }

  @Override
  public synchronized void stopReporter() {
    if (libratoReporter != null) {
      LOG.info("stopping Librato metrics reporter");
      libratoReporter.shutdown();
      libratoReporter = null;
    }
  }
}
