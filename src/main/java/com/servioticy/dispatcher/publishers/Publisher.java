/*******************************************************************************
 * Copyright 2015 Barcelona Supercomputing Center (BSC)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.servioticy.dispatcher.publishers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public abstract class Publisher {

	public static Publisher factory(String className, String address, int port, String taskId)
			throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException,
			InvocationTargetException {
		Class<?> c = Class.forName(className);
		Constructor<?> constructor = c.getConstructor(String.class, Integer.class, String.class);
		return (Publisher) constructor.newInstance(address, port, taskId);
	}
	public abstract void connect(String username, String password) throws Exception;
	public abstract void close() throws Exception;
	public abstract boolean isConnected();
	public abstract void publishMessage(String topic, String message) throws Exception;
	
}
