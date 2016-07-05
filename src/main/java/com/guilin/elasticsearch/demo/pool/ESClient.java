package com.guilin.elasticsearch.demo.pool;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;


public class ESClient implements IDroplet<Client> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ESClient.class);

	private Client client;

	public ESClient(final Set<String> eshost, final int esport, final String timeout, final String esName) {
		initClient(eshost, esport, timeout, esName);
	}

	@SuppressWarnings("resource")
	private void initClient(final Set<String> eshost, final int esport, final String timeout, final String esName) {

		Settings settings = Settings.settingsBuilder()
				.put("client.transport.ping_timeout", timeout)
				.put("cluster.name", esName)
				.put("client.transport.sniff", true)
				.build();


		if (CollectionUtils.isEmpty(eshost)) {
			throw new IllegalArgumentException("eshost is null");
		}

		InetSocketTransportAddress[] address = new InetSocketTransportAddress[eshost.size()];
		int i = 0;
		for (String host : eshost) {
			try {
				address[i] = new InetSocketTransportAddress(InetAddress.getByName(host), esport);
			} catch (UnknownHostException e) {
				LOGGER.error(e.getMessage(), e);
			}
			i++;
		}
		client = TransportClient.builder().settings(settings).addPlugin(DeleteByQueryPlugin.class).build().addTransportAddresses(address);
	}

	@Override
	public void close() throws IOException {
		client.close();
	}

	@Override
	public Client illusion() {
		return client;
	}

	@Override
	public boolean valid() {
		if (client != null) {
			return true;
		} else {
			return false;
		}
	}
}