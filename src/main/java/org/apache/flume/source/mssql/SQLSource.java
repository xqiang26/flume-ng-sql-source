package org.apache.flume.source.mssql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.flume.source.mssql.metrics.SqlSourceCounter;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Source to read data from a SQL database. This source ask for new data in a table each configured time.<p>
 * 
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 */
public class SQLSource extends AbstractPollableSource implements Configurable {
    
    private static final Logger LOG = LoggerFactory.getLogger(SQLSource.class);
    protected SQLSourceHelper sqlSourceHelper;
    private SqlSourceCounter sqlSourceCounter;
    private ChannelWriter customWriter;
    private HibernateHelper hibernateHelper;
       
    /**
     * Configure the source, load configuration properties and establish connection with database
     */
    @Override
    public void doConfigure(Context context) {
    	
    	LOG.getName();
        	
    	LOG.info("Reading and processing configuration values for source " + getName());
		
    	/* Initialize configuration parameters */
    	sqlSourceHelper = new SQLSourceHelper(context, this.getName());
        
    	/* Initialize metric counters */
		sqlSourceCounter = new SqlSourceCounter("SOURCESQL." + this.getName());
        
        /* Establish connection with database */
        hibernateHelper = new HibernateHelper(sqlSourceHelper);
        hibernateHelper.establishSession();
       
        /* Instantiate the custom Writer */
        customWriter = new ChannelWriter();
        
    }  
    
    /**
     * Process a batch of events performing SQL Queries
     */
	@Override
	public Status doProcess() throws EventDeliveryException {
		
		try {
			sqlSourceCounter.startProcess();			
			
			List<Map<String,Object>> result = hibernateHelper.executeQuery();
						
			if (!result.isEmpty())
			{
				customWriter.write(result);
				customWriter.flush();
				sqlSourceCounter.incrementEventCount(result.size());
				
				sqlSourceHelper.updateStatusFile();
			}
			
			sqlSourceCounter.endProcess(result.size());
			
			if (result.size() < sqlSourceHelper.getMaxRows()){
				Thread.sleep(sqlSourceHelper.getRunQueryDelay());
			}
						
			return Status.READY;
			
		} catch (IOException | InterruptedException e) {
			LOG.error("Error procesing row", e);
			return Status.BACKOFF;
		}
	}
 
	/**
	 * Starts the source. Starts the metrics counter.
	 */
	@Override
    public void doStart() {
        
    	LOG.info("Starting sql source {} ...", getName());
        sqlSourceCounter.start();
    }

	/**
	 * Stop the source. Close database connection and stop metrics counter.
	 */
    @Override
    public void doStop() {
        
        LOG.info("Stopping sql source {} ...", getName());
        
        try 
        {
            hibernateHelper.closeSession();
            customWriter.close();
        } catch (IOException e) {
        	LOG.warn("Error ChannelWriter object ", e);
        } finally {
        	this.sqlSourceCounter.stop();
        }
    }
    
    private class ChannelWriter{
        private List<Event> events = new ArrayList<>();

        public void write(List<Map<String, Object>> mapAlls) throws IOException {
            for (Map<String, Object> map : mapAlls) {
            	Event event = new SimpleEvent();
            	String jsonBody = JSONValue.toJSONString(map);
            	event.setBody(jsonBody.getBytes());
            	
                Map<String, String> headers = new HashMap<String, String>();
    			headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
    			event.setHeaders(headers);
    			
                events.add(event);
                if (events.size() >= sqlSourceHelper.getBatchSize())
                	flush();
            }
        }

        public void flush() throws IOException {
            getChannelProcessor().processEventBatch(events);
            events.clear();
        }

        public void close() throws IOException {
            flush();
        }
    }
}