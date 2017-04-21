package org.apache.flume.source.mssql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.hibernate.CacheMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to manage hibernate sessions and perform queries
 * 
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 *
 */
public class HibernateHelper {

	private static final Logger LOG = LoggerFactory
			.getLogger(HibernateHelper.class);

	private static SessionFactory factory;
	private Session session;
	private Configuration config;
	private SQLSourceHelper sqlSourceHelper;

	/**
	 * Constructor to initialize hibernate configuration parameters
	 * @param sqlSourceHelper Contains the configuration parameters from flume config file
	 */
	public HibernateHelper(SQLSourceHelper sqlSourceHelper) {

		this.sqlSourceHelper = sqlSourceHelper;
		Context context = sqlSourceHelper.getContext();
		
		Map<String,String> hibernateProperties = context.getSubProperties("hibernate.");
		Iterator<Map.Entry<String,String>> it = hibernateProperties.entrySet().iterator();
		
		config = new Configuration();
		Map.Entry<String, String> e;
		
		while (it.hasNext()){
			e = it.next();
			config.setProperty("hibernate." + e.getKey(), e.getValue());
		}
	}

	/**
	 * Connect to database using hibernate
	 */
	public void establishSession() {

		LOG.info("Opening hibernate session");

		factory = config.buildSessionFactory(); 
		session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
		
		session.setDefaultReadOnly(sqlSourceHelper.isReadOnlySession());
	}

	/**
	 * Close database connection
	 */
	public void closeSession() {

		LOG.info("Closing hibernate session");

		session.close();
		factory.close();
	}

	/**
	 * Execute the selection query in the database
	 * @return The query result. Each Object is a cell content. <p>
	 * The cell contents use database types (date,int,string...), 
	 * keep in mind in case of future conversions/castings.
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,Object>> executeQuery() throws InterruptedException {
		
		List<Map<String,Object>> rowsList = new ArrayList<Map<String,Object>>() ;
		Query query;
		
		if (!session.isConnected()){
			resetConnection();
		}
				
		if (sqlSourceHelper.isCustomQuerySet()){
			
			query = session.createSQLQuery(sqlSourceHelper.buildQuery());
			
			if (sqlSourceHelper.getMaxRows() != 0){
				query = query.setMaxResults(sqlSourceHelper.getMaxRows());
			}			
		}
		else
		{
			query = session
					.createSQLQuery(sqlSourceHelper.getQuery())
					.setFirstResult(Integer.parseInt(sqlSourceHelper.getCurrentIndex()));
			
			if (sqlSourceHelper.getMaxRows() != 0){
				query = query.setMaxResults(sqlSourceHelper.getMaxRows());
			}
		}
		
		try {
			rowsList = query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
			//rowsList = query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.TO_LIST).list();
		}catch (Exception e){
			resetConnection();
		}
		
		if (!rowsList.isEmpty()) {
			if (sqlSourceHelper.isCustomQuerySet()){
					LOG.info(rowsList.get(rowsList.size()-1).toString());
					sqlSourceHelper.setCurrentIndex(rowsList.get(rowsList.size()-1).get("TMSTAMP").toString());
//					sqlSourceHelper.setCurrentIndex(rowsList.get(rowsList.size()-1).get(rowsList.get(rowsList.size()-1).size() - 1).toString());
			}
			else
			{
				sqlSourceHelper.setCurrentIndex(Integer.toString((Integer.parseInt(sqlSourceHelper.getCurrentIndex())
						+ rowsList.size())));
			}
		}
		
		return rowsList;
	}

	private void resetConnection() throws InterruptedException{
		session.close();
		factory.close();
		establishSession();
	}
}