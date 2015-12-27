package com.servioticy.dispatcher.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.datamodel.reputation.OnBehalf;
import com.servioticy.datamodel.reputation.Reputation;
import com.servioticy.dispatcher.DispatcherContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created by alvaro on 26/12/15.
 */
public class ReputationBolt implements IRichBolt {
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private TopologyContext context;
    private DispatcherContext dc;
    ObjectMapper mapper;
    private static Logger LOG = org.apache.log4j.Logger.getLogger(ReputationBolt.class);


    public ReputationBolt(DispatcherContext dc) {
        this.dc = dc;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        mapper = new ObjectMapper();
        this.collector = collector;
        this.context = context;
    }

    @Override
    public void execute(Tuple input) {
        String reputationDoc = input.getStringByField("reputation");
        Reputation reputation = null;
        try {
            reputation = this.mapper.readValue(reputationDoc, Reputation.class);
        } catch (IOException e) {
            LOG.warn("Failed to deserialize reputation document: \n" + reputationDoc, e);
            collector.ack(input);
            return;
        }

        reputation.setTimestamp(System.currentTimeMillis());

        if(reputation.getAction().equals(Reputation.ACTION_WRITE)){
            if(reputation.getOnBehalf().getType().equals(OnBehalf.TYPE_STREAM)){
                // TODO GET (onBehalf) SO
                // TODO reputation.getOnBehalf().setUserId(so.getUserId());
            }
        }
        else if(reputation.getAction().equals(Reputation.ACTION_READ)){
            if(reputation.getOnBehalf().getType().equals(OnBehalf.TYPE_STREAM) ||
                    reputation.getOnBehalf().getType().equals(OnBehalf.TYPE_PUBSUB_EXTERNAL) ||
                    reputation.getOnBehalf().getType().equals(OnBehalf.TYPE_PUBSUB_INTERNAL)){
                // TODO GET SO
                // TODO reputation.setOwnerId(so.getUserId());
            }
        }

        // TODO Send to reputation bucket

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
