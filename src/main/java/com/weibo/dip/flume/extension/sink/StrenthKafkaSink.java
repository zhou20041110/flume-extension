/**
 * 
 */
package com.weibo.dip.flume.extension.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author yurun
 *
 */
public class StrenthKafkaSink extends AbstractSink implements Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

	private Properties kafkaProps;

	private int consumers;

	private ThreadPoolExecutor executor;

	private boolean consumerStoped = false;

	private int batchSize;

	private KafkaSinkCounter counter;

	@Override
	public Status process() throws EventDeliveryException {
		try {
			Thread.sleep(3 * 1000);
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

				String eventTopic = null;

				String eventKey = null;

				while (!consumerStoped) {
					try {
						transaction = channel.getTransaction();

						transaction.begin();

						messageList.clear();

						for (int processedEvents = 0; processedEvents < batchSize; processedEvents += 1) {
							event = channel.take();

							if (event == null) {
								// no events available in channel
								break;
							}

							eventTopic = "flume_test_topic";

							eventKey = String.valueOf(System.currentTimeMillis());

							byte[] eventBody = event.getBody();

							// create a message and add to buffer
							KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(eventTopic, eventKey,
									eventBody);

							messageList.add(data);
						}

						transaction.commit();

						transaction.close();

						// publish batch and commit.
						if (CollectionUtils.isNotEmpty(messageList)) {
							long startTime = System.nanoTime();

							producer.send(messageList);

							long endTime = System.nanoTime();

							counter.addToKafkaEventSendTimer((endTime - startTime) / (1000 * 1000));
							counter.addToEventDrainSuccessCount(Long.valueOf(messageList.size()));
						} else {
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {
							}
						}
					} catch (Exception e) {
						LOGGER.error("Failed to publish events: {}", ExceptionUtils.getFullStackTrace(e));

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
				LOGGER.error("channel consumer error: " + ExceptionUtils.getFullStackTrace(e));
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

		super.start();

		executor = new ThreadPoolExecutor(consumers, consumers, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>());

		for (int index = 0; index < consumers; index++) {
			executor.submit(new ChannelConsumer());
		}
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
		consumers = context.getInteger("consumers");
		LOGGER.info("consumers: {}", consumers);

		batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE, KafkaSinkConstants.DEFAULT_BATCH_SIZE);
		LOGGER.info("batchSize: " + batchSize);

		kafkaProps = KafkaSinkUtil.getKafkaProperties(context);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Kafka producer properties: " + kafkaProps);
		}

		if (counter == null) {
			counter = new KafkaSinkCounter(getName());
		}
	}

}
