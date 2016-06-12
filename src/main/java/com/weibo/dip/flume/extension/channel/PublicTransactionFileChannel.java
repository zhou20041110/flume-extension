/**
 * 
 */
package com.weibo.dip.flume.extension.channel;

import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.file.FileChannel;

/**
 * @author yurun
 *
 */
public class PublicTransactionFileChannel extends FileChannel {

	@Override
	public BasicTransactionSemantics createTransaction() {
		return super.createTransaction();
	}

}
