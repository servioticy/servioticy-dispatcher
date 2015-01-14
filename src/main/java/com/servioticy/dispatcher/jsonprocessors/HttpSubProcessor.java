/*******************************************************************************
 * Copyright 2014 Barcelona Supercomputing Center (BSC)
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
package com.servioticy.dispatcher.jsonprocessors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.subscription.HttpSubscription;
import com.servioticy.restclient.RestClient;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class HttpSubProcessor {
    private JsonPathReplacer url;
    private HashMap<JsonPathReplacer, JsonPathReplacer> headers;
    private JsonPathReplacer body;
    private int method;
    private HttpSubscription httpSubs;
    private ObjectMapper mapper;

    private AliasReplacer aliases;

    public HttpSubProcessor(HttpSubscription httpSubs, ObjectMapper mapper) {
        this.aliases = new AliasReplacer(httpSubs.getAliases());
        this.httpSubs = httpSubs;
        this.mapper = mapper;

        String subsMethod = httpSubs.getMethod().toUpperCase();
        if (subsMethod.equals("GET")) {
            this.method = RestClient.GET;
        } else if (subsMethod.equals("PUT")) {
            this.method = RestClient.PUT;
        } else {
            this.method = RestClient.POST;
        }
    }

    public HttpSubscription replaceAliases() {

        this.httpSubs.setDestination(aliases.replace(httpSubs.getDestination()));
        this.httpSubs.setBody(aliases.replace(httpSubs.getBody()));

        if (this.httpSubs.getHeaders() != null) {
            LinkedHashMap<String, String> headers = new LinkedHashMap<String, String>();
            for (Entry<String, String> ppheader : this.httpSubs.getHeaders().entrySet()) {
                headers.put(aliases.replace(ppheader.getKey()), aliases.replace(ppheader.getValue()));
            }
            this.httpSubs.setHeaders(headers);
        }

        return this.httpSubs;
    }

    public void compileJSONPaths() {
        this.url = new JsonPathReplacer(httpSubs.getDestination(), this.mapper);
        this.body = null;
        if (httpSubs.getBody() != null) {
            this.body = new JsonPathReplacer(httpSubs.getBody(), this.mapper);
        }


        this.headers = new HashMap<JsonPathReplacer, JsonPathReplacer>();
        if (httpSubs.getHeaders() != null) {
            for (Entry<String, String> ppheader : httpSubs.getHeaders().entrySet()) {
                this.headers.put(new JsonPathReplacer(ppheader.getKey(), this.mapper), new JsonPathReplacer(ppheader.getValue(), this.mapper));
            }
        }

    }

    public int getMethod() {
        return method;
    }

    public String getUrl(String inputJson) {
        Map<String, String> inputJsons = new HashMap<String, String>();
        inputJsons.put("", inputJson);

        return this.url.replace(inputJsons);
    }

    public String getBody(String inputJson) {
        if (this.body == null) {
            return null;
        }
        Map<String, String> inputJsons = new HashMap<String, String>();
        inputJsons.put("", inputJson);
        return this.body.replace(inputJsons);
    }

    public HashMap<String, String> getHeaders(String inputJson) {
        Map<String, String> inputJsons = new HashMap<String, String>();
        inputJsons.put("", inputJson);

        HashMap<String, String> headers = new HashMap<String, String>();

        for (Entry<JsonPathReplacer, JsonPathReplacer> ppheader : this.headers.entrySet()) {
            headers.put(ppheader.getKey().replace(inputJsons), ppheader.getValue().replace(inputJsons));
        }

        return headers;
    }
}
