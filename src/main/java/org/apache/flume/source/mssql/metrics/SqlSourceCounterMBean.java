package org.apache.flume.source.mssql.metrics;

/**
 * 
 * @author <a href="mailto:lalazaro@keedio.com">Luis Lazaro</a>
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 */
public interface SqlSourceCounterMBean {
    public long getEventCount();
    public void incrementEventCount(int value);
    public long getAverageThroughput();
    public long getCurrentThroughput();
    public long getMaxThroughput();
}