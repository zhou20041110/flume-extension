/**
 * 
 */
package com.weibo.dip.flume.extension.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.kafka.KafkaSink;
import org.apache.flume.sink.kafka.KafkaSinkConstants;
import org.apache.flume.sink.kafka.KafkaSinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author yurun
 *
 */
public class MultithreadingKafkaSink extends AbstractSink implements Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

	private Properties kafkaProps;

	private long processSleep;

	private String topicHeaderName;

	private int consumers;

	private long batchSleep;

	private ThreadPoolExecutor executor;

	private boolean consumerStoped = false;

	private int batchSize;

	private KafkaSinkCounter counter;

	@Override
	public Status process() throws EventDeliveryException {
		try {
			Thread.sleep(processSleep);
		} catch (InterruptedException e) {
		}

		return Status.READY;
	}

	public class ChannelConsumer implements Runnable {

		private List<KeyedMessage<String, byte[]>> messageList = new ArrayList<>();

		@Override
		public void run() {
			Producer<String, byte[]> producer = null;

			try {
				ProducerConfig config = new ProducerConfig(kafkaProps);

				producer = new Producer<String, byte[]>(config);

				Channel channel = getChannel();

				Transaction transaction = null;

				Event event = null;

				Map<String, String> headers = null;

				String eventTopic = null;

				String eventKey = null;

				while (!consumerStoped) {
					try {
						transaction = channel.getTransaction();

						transaction.begin();

						messageList.clear();

						for (int processedEvents = 0; processedEvents < batchSize; processedEvents++) {
							event = channel.take();

							if (event == null) {
								break;
							}

							headers = event.getHeaders();

							if (MapUtils.isNotEmpty(headers) && headers.containsKey(topicHeaderName)) {
								eventTopic = headers.get(topicHeaderName);
							} else {
								LOGGER.warn("Event header names do not contain {}", topicHeaderName);

								continue;
							}

							eventKey = String.valueOf(System.currentTimeMillis());

							byte[] eventBody = event.getBody();

							// create a message and add to buffer
							KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(eventTopic, eventKey,
									eventBody);

							messageList.add(data);
						}

						transaction.commit();

						transaction.close();

						// send batch
						if (CollectionUtils.isNotEmpty(messageList)) {
							long startTime = System.nanoTime();

							producer.send(messageList);

							long endTime = System.nanoTime();

							counter.addToKafkaEventSendTimer((endTime - startTime) / (1000 * 1000));
							counter.addToEventDrainSuccessCount(Long.valueOf(messageList.size()));
						} else {
							try {
								Thread.sleep(batchSleep);
							} catch (InterruptedException e) {
							}
						}
					} catch (Exception e) {
						LOGGER.error("Failed to send events: {}", ExceptionUtils.getFullStackTrace(e));

						if (transaction != null) {
							try {
								transaction.rollback();

								transaction.close();

								counter.incrementRollbackCount();
							} catch (Exception ex) {
								LOGGER.error("Transaction rollback failed: {}", ExceptionUtils.getFullStackTrace(ex));
							}
						}
					}
				}
			} catch (Exception e) {
				LOGGER.error("Channel consumer error: " + ExceptionUtils.getFullStackTrace(e));
			} finally {
				if (producer != null) {
					producer.close();
				}
			}
		}

	}

	@Override
	public synchronized void start() {
		counter.start();

		executor = new ThreadPoolExecutor(consumers, consumers, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>());

		for (int index = 0; index < consumers; index++) {
			executor.submit(new ChannelConsumer());
		}

		super.start();
	}

	@Override
	public synchronized void stop() {
		consumerStoped = true;

		executor.shutdown();

		while (!executor.isTerminated()) {
			try {
				executor.awaitTermination(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
		}

		counter.stop();

		LOGGER.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);

		super.stop();
	}

	/**
	 * We configure the sink and generate properties for the Kafka Producer
	 *
	 * Kafka producer properties is generated as follows: 1. We generate a
	 * properties object with some static defaults that can be overridden by
	 * Sink configuration 2. We add the configuration users added for Kafka
	 * (parameters starting with .kafka. and must be valid Kafka Producer
	 * properties 3. We add the sink's documented parameters which can override
	 * other properties
	 *
	 * @param context
	 */
	@Override
	public void configure(Context context) {
		processSleep = context.getLong("processSleep", 3000L);
		LOGGER.info("processSleep: {}", processSleep);

		Preconditions.checkState(processSleep > 0, "processSleep's value must be greater than zero");

		topicHeaderName = context.getString("topicHeaderName");
		LOGGER.info("topicHeaderName: {}", topicHeaderName);

		Preconditions.checkState(StringUtils.isNotEmpty(topicHeaderName), "topicHeaderName's value must not be empty");

		consumers = context.getInteger("consumers");
		LOGGER.info("consumers: {}", consumers);

		Preconditions.checkState(consumers > 0, "consumers's value must be greater than zero");

		batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE, KafkaSinkConstants.DEFAULT_BATCH_SIZE);
		LOGGER.info("batchSize: {}" + batchSize);

		Preconditions.checkState(batchSize > 0, "batchSize's value must be greater than zero");

		batchSleep = context.getLong("batchSleep", 100L);
		LOGGER.info("batchSleep: {}", batchSleep);

		Preconditions.checkState(batchSleep > 0, "batchSleep's value must be greater than zero");

		kafkaProps = KafkaSinkUtil.getKafkaProperties(context);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Kafka producer properties: " + kafkaProps);
		}

		if (counter == null) {
			counter = new KafkaSinkCounter(getName());
		}
	}

}
