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
package com.jointsky.bigdata.api.test;


import com.jointsky.bigdata.api.EdpsKafkaService;
import com.jointsky.bigdata.api.EdpsKafkaServiceImpl;
import com.jointsky.bigdata.api.MessageData;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;


/**
 * kafka 消息发送测试
 */
public class EdpsKafkaClientTest {
	static EdpsKafkaService edpsKafkaService;

	static List<MessageData> logs;

	@BeforeClass
	public static void setup() throws Exception {

		edpsKafkaService = new EdpsKafkaServiceImpl();
		edpsKafkaService.establishConnect();
		logs = new ArrayList<>();
		for (int i = 0; i < 100000; i++) {
			//构造 发送消息,每条消息指定到具体的topic上
			logs.add(new MessageData().setData("This is a jointframe log line.  Number " + i).setResourceName("std-std-none"));
		}
	}

	@AfterClass
	public static void cleanup() throws Exception {
		edpsKafkaService.closeConnect();
	}


	// 测试发送消息
	@Test
	public void testProducerSendMessage() throws Throwable {
		edpsKafkaService.send(logs);
	}

}
