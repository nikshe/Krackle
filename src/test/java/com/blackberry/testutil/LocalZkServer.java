/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.blackberry.testutil;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalZkServer {

	private static final Logger LOG = LoggerFactory
		 .getLogger(LocalZkServer.class);

	private final int clientPort = 21818; // non-standard
	private final int numConnections = 5000;
	private final int tickTime = 2000;

	private Class<?> factoryClass;
	private Object standaloneServerFactory;
	private File dir;

	private ZooKeeperServer server;

	public LocalZkServer() throws InstantiationException, IllegalAccessException,
		 SecurityException, NoSuchMethodException, IllegalArgumentException,
		 InvocationTargetException, ClassNotFoundException, IOException {
		String dataDirectory = System.getProperty("java.io.tmpdir");

		dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();

		while (dir.exists()) {
			LOG.info("deleting {}", dir);
			FileUtils.deleteDirectory(dir);
		}

		server = new ZooKeeperServer(dir, dir, tickTime);

		// The class that we need changed name between CDH3 and CDH4, so let's
		// check
		// for the right version here.
		try {
			factoryClass = Class
				 .forName("org.apache.zookeeper.server.NIOServerCnxnFactory");

			standaloneServerFactory = factoryClass.newInstance();
			Method configure = factoryClass.getMethod("configure",
				 InetSocketAddress.class, Integer.TYPE);
			configure.invoke(standaloneServerFactory, new InetSocketAddress(
				 clientPort), numConnections);
			Method startup = factoryClass.getMethod("startup", ZooKeeperServer.class);
			startup.invoke(standaloneServerFactory, server);

		} catch (ClassNotFoundException e) {
			LOG.info("Did not find NIOServerCnxnFactory");
			try {
				factoryClass = Class
					 .forName("org.apache.zookeeper.server.NIOServerCnxn$Factory");

				Constructor<?> constructor = factoryClass.getConstructor(
					 InetSocketAddress.class, Integer.TYPE);
				standaloneServerFactory = constructor.newInstance(
					 new InetSocketAddress(clientPort), numConnections);
				Method startup = factoryClass.getMethod("startup",
					 ZooKeeperServer.class);
				startup.invoke(standaloneServerFactory, server);

			} catch (ClassNotFoundException e1) {
				LOG.info("Did not find NIOServerCnxn.Factory");
				throw new ClassNotFoundException(
					 "Can't find NIOServerCnxnFactory or NIOServerCnxn.Factory");
			}
		}
	}

	public void shutdown() throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, SecurityException,
		 NoSuchMethodException, IOException {
		server.shutdown();

		Method shutdown = factoryClass.getMethod("shutdown", new Class<?>[]{});
		shutdown.invoke(standaloneServerFactory, new Object[]{});

		while (dir.exists()) {
			LOG.info("deleting {}", dir);
			FileUtils.deleteDirectory(dir);
		}
	}

	public Class<?> getFactoryClass() {
		return factoryClass;
	}

	public void setFactoryClass(Class<?> factoryClass) {
		this.factoryClass = factoryClass;
	}

	public Object getStandaloneServerFactory() {
		return standaloneServerFactory;
	}

	public void setStandaloneServerFactory(Object standaloneServerFactory) {
		this.standaloneServerFactory = standaloneServerFactory;
	}

	public File getDir() {
		return dir;
	}

	public int getClientport() {
		return clientPort;
	}

	public int getNumconnections() {
		return numConnections;
	}

	public int getTicktime() {
		return tickTime;
	}

	public ZooKeeperServer getServer() {
		return server;
	}

}
