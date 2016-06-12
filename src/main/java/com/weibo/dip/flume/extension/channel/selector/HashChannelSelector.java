/**
 * 
 */
package com.weibo.dip.flume.extension.channel.selector;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.AbstractChannelSelector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author yurun
 *
 */
public class HashChannelSelector extends AbstractChannelSelector {

	private static final List<Channel> EMPTY_CHANNELS = new ArrayList<>();

	@Override
	public List<Channel> getRequiredChannels(Event event) {
		Preconditions.checkState(CollectionUtils.isNotEmpty(getAllChannels()), "getAllChannels() is empty");

		long code = event.getBody() == null ? System.currentTimeMillis() : event.getBody().hashCode();

		Channel channel = getAllChannels().get((int) (code % getAllChannels().size()));

		return Lists.newArrayList(channel);
	}

	@Override
	public List<Channel> getOptionalChannels(Event event) {
		return EMPTY_CHANNELS;
	}

	@Override
	public void configure(Context context) {

	}

}
