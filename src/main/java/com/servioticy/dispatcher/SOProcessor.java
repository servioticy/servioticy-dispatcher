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
package com.servioticy.dispatcher;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.servioticy.datamodel.serviceobject.SO;
import com.servioticy.datamodel.serviceobject.SO010;
import com.servioticy.datamodel.serviceobject.SO020;
import com.servioticy.datamodel.sensorupdate.SensorUpdate;

import javax.script.ScriptException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public abstract class SOProcessor {
    protected SOProcessor(){}

    public static SOProcessor factory(SO so) throws IOException {
        if(so.getClass() == SO010.class) {
            return new SOProcessor010((SO010) so);
        }
        if(so.getClass() == SO020.class) {
            return new SOProcessor020((SO020) so);
        }
        return null;
    }

    public abstract SensorUpdate getResultSU(String streamId, Map<String, SensorUpdate> inputSUs, String origin, long timestamp) throws JsonParseException, JsonMappingException, IOException, ScriptException;
    public abstract Set<String> getSourceIdsByStream(String streamId);
    public abstract Set<String> getStreamsBySourceId(String docId);
}
