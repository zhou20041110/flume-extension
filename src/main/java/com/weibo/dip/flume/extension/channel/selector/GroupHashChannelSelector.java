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
import org.apache.commons.collections.MapUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.AbstractChannelSelector;

import com.google.common.base.Preconditions;

/**
 * @author yurun
 *
 */
public class GroupHashChannelSelector extends AbstractChannelSelector {

	private static final List<Channel> EMPTY_CHANNELS = new ArrayList<>();

	private Map<String, List<Channel>> groupChannels = null;

	@Override
	public List<Channel> getRequiredChannels(Event event) {
		Preconditions.checkState(MapUtils.isNotEmpty(groupChannels), "groupChannels is empty");

		long code = event.getBody() == null ? System.currentTimeMillis() : event.getBody().hashCode();

		List<Channel> requiredChannels = new ArrayList<>();

		for (Entry<String, List<Channel>> entry : groupChannels.entrySet()) {
			String group = entry.getKey();
			List<Channel> channels = entry.getValue();

			Preconditions.checkState(CollectionUtils.isNotEmpty(channels), "group " + group + " channels is empty");

			requiredChannels.add(channels.get((int) (code % channels.size())));
		}

		return requiredChannels;
	}

	@Override
	public List<Channel> getOptionalChannels(Event event) {
		return EMPTY_CHANNELS;
	}

	@Override
	public void configure(Context context) {
		Preconditions.checkState(CollectionUtils.isNotEmpty(getAllChannels()), "getAllChannels() is empty");

		groupChannels = new HashMap<>();

		for (Channel channel : getAllChannels()) {
			String channelName = channel.getName();

			String groupName = channelName.split("_")[0];

			if (!groupChannels.containsKey(groupName)) {
				groupChannels.put(groupName, new ArrayList<>());
			}

			groupChannels.get(groupName).add(channel);
		}
	}

}
