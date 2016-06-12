/**
 * 
 */
package com.weibo.dip.flume.extension.channel.selector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.AbstractChannelSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @author yurun
 *
 */
public class ThreadChannelSelector extends AbstractChannelSelector {

	private static final Logger LOGGER = LoggerFactory.getLogger(ThreadChannelSelector.class);

	private static final List<Channel> EMPTY_CHANNELS = new ArrayList<>();

	private List<List<Channel>> groupChannels = new ArrayList<>();

	private Map<Long, List<Channel>> threadChannels = new HashMap<>();

	@Override
	public List<Channel> getRequiredChannels(Event event) {
		long id = Thread.currentThread().getId();
		
		if (threadChannels.containsKey(id)) {
			return threadChannels.get(id);
		} else {
			Preconditions.checkState(CollectionUtils.isNotEmpty(groupChannels),
					"can't allocate group channels for thread " + id
							+ ", maybe sourceThreads's size inconsistence with groupChannels's size");

			List<Channel> channels = groupChannels.remove(0);

			threadChannels.put(id, channels);

			return channels;
		}
	}

	@Override
	public List<Channel> getOptionalChannels(Event event) {
		return EMPTY_CHANNELS;
	}

	@Override
	public void configure(Context context) {
		Preconditions.checkState(CollectionUtils.isNotEmpty(getAllChannels()), "getAllChannels() is empty");

		groupChannels.clear();

		threadChannels.clear();

		Map<String, List<Channel>> groupNameChannels = new HashMap<>();

		for (Channel channel : getAllChannels()) {
			String channelName = channel.getName();

			String groupName = channelName.split("_")[0];

			LOGGER.info("channelName: " + channelName + ", groupName: " + groupName);

			if (!groupNameChannels.containsKey(groupName)) {
				groupNameChannels.put(groupName, new ArrayList<>());
			}

			groupNameChannels.get(groupName).add(channel);
		}

		int groupChannelSize = -1;

		for (Entry<String, List<Channel>> entry : groupNameChannels.entrySet()) {
			if (groupChannelSize == -1) {
				groupChannelSize = entry.getValue().size();
			} else {
				Preconditions.checkState(groupChannelSize == entry.getValue().size(), "group " + entry.getKey()
						+ " channel's size(" + entry.getValue().size() + ") inconsistence with " + groupChannelSize);
			}
		}

		for (int index = 0; index < groupChannelSize; index++) {
			List<Channel> channels = new ArrayList<>();

			for (Entry<String, List<Channel>> entry : groupNameChannels.entrySet()) {
				channels.add(entry.getValue().get(index));
			}

			groupChannels.add(channels);
		}
	}

}
