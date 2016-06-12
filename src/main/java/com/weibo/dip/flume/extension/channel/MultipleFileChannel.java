/**
 * 
 */
package com.weibo.dip.flume.extension.channel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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
public class MultipleFileChannel extends BasicChannelSemantics {

	private static final Logger LOGGER = LoggerFactory.getLogger(MultipleFileChannel.class);

	private int channels;

	private List<StrenthFileChannel> fileChannels = new ArrayList<>();

	public List<StrenthFileChannel> getFileChannels() {
		return fileChannels;
	}

	@Override
	public void configure(Context context) {
		channels = context.getInteger("channels");

		String checkpointDirStr = context.getString("checkpointDir");

		String dataDirStr = context.getString("dataDir");

		LOGGER.info("channels: {}", channels);

		LOGGER.info("checkpointDir: {}", checkpointDirStr);

		LOGGER.info("dataDir: {}", dataDirStr);

		Preconditions.checkState(channels > 0, "channels value must be more than zero");

		File checkpointDir = new File(checkpointDirStr);

		if (!checkpointDir.exists()) {
			checkpointDir.mkdirs();
		}

		File dataDir = new File(dataDirStr);

		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}

		for (int index = 0; index < channels; index++) {
			StrenthFileChannel fileChannel = new StrenthFileChannel();

			fileChannel.setName("fileChannel_" + index);

			Context ctx = new Context();

			ctx.put(FileChannelConfiguration.CHECKPOINT_DIR,
					new File(checkpointDir, String.valueOf(index)).getAbsolutePath());
			ctx.put(FileChannelConfiguration.DATA_DIRS, new File(dataDir, String.valueOf(index)).getAbsolutePath());

			fileChannel.configure(ctx);

			fileChannels.add(fileChannel);
		}
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
