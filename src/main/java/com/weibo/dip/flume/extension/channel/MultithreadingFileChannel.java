/**
 * 
 */
package com.weibo.dip.flume.extension.channel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @author yurun
 *
 */
public class MultithreadingFileChannel extends BasicChannelSemantics {

	private static final Logger LOGGER = LoggerFactory.getLogger(MultithreadingFileChannel.class);

	private int channels;

	private List<PublicTransactionFileChannel> fileChannels = new ArrayList<>();

	@Override
	public void configure(Context context) {
		channels = context.getInteger("channels");
		LOGGER.info("channels: {}", channels);

		Preconditions.checkState(channels > 0, "channels's value must be greater than zero");

		String checkpointDirStr = context.getString("checkpointDir");
		LOGGER.info("checkpointDir: {}", checkpointDirStr);

		Preconditions.checkState(StringUtils.isNotEmpty(checkpointDirStr),
				"checkpointDirStr's value must not be empty");

		File checkpointDir = new File(checkpointDirStr);

		if (!checkpointDir.exists()) {
			checkpointDir.mkdirs();
		}

		String dataDirStr = context.getString("dataDir");
		LOGGER.info("dataDir: {}", dataDirStr);

		Preconditions.checkState(StringUtils.isNotEmpty(dataDirStr), "dataDirStr's value must not be empty");

		File dataDir = new File(dataDirStr);

		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}

		for (int index = 0; index < channels; index++) {
			PublicTransactionFileChannel fileChannel = new PublicTransactionFileChannel();

			fileChannel.setName("EnhancedFileChannel_" + index);

			Context ctx = new Context();

			ctx.put(FileChannelConfiguration.CHECKPOINT_DIR,
					new File(checkpointDir, String.valueOf(index)).getAbsolutePath());
			ctx.put(FileChannelConfiguration.DATA_DIRS, new File(dataDir, String.valueOf(index)).getAbsolutePath());

			fileChannel.configure(ctx);

			fileChannels.add(fileChannel);
		}

		LOGGER.info("MultipleFileChannel configure success");
	}

	@Override
	public synchronized void start() {
		for (FileChannel fileChannel : fileChannels) {
			fileChannel.start();
		}

		super.start();
	}

	@Override
	protected BasicTransactionSemantics createTransaction() {
		return fileChannels.get((int) (System.currentTimeMillis() % channels)).createTransaction();
	}

	@Override
	public synchronized void stop() {
		for (FileChannel fileChannel : fileChannels) {
			fileChannel.stop();
		}

		super.stop();
	}

}
