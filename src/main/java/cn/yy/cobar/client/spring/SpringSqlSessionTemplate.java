package cn.yy.cobar.client.spring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

import cn.yy.cobar.client.audit.ISqlAuditor;
import cn.yy.cobar.client.exception.UncategorizedCobarClientException;
import cn.yy.cobar.client.router.ICobarRouter;
import cn.yy.cobar.client.router.support.IBatisRoutingFact;
import cn.yy.cobar.client.sessionfactory.CobarSessionFactoryDescriptor;
import cn.yy.cobar.client.sessionfactory.ICobarSessionFactoryService;
import cn.yy.cobar.client.support.execution.ConcurrentRequest;
import cn.yy.cobar.client.support.execution.DefaultConcurrentRequestProcessor;
import cn.yy.cobar.client.support.execution.IConcurrentRequestProcessor;
import cn.yy.cobar.client.support.utils.CollectionUtils;
import cn.yy.cobar.client.support.utils.MapUtils;

public class SpringSqlSessionTemplate extends SqlSessionTemplate implements DisposableBean, InitializingBean {

	private transient Logger logger = LoggerFactory.getLogger(SpringSqlSessionTemplate.class);

	private ICobarSessionFactoryService cobarDataSourceService;

	private ICobarRouter<IBatisRoutingFact> router;

	private IConcurrentRequestProcessor concurrentRequestProcessor;

	/**
	 * setup ExecutorService for data access requests on each data sources.<br>
	 * map key(String) is the identity of DataSource; map value(ExecutorService)
	 * is the ExecutorService that will be used to execute query requests on the
	 * key's data source.
	 */
	private Map<String, ExecutorService> dataSourceSpecificExecutors = new HashMap<String, ExecutorService>();

	private List<ExecutorService> internalExecutorServiceRegistry = new ArrayList<ExecutorService>();

	/**
	 * if you want to do SQL auditing, inject an {@link ISqlAuditor} for use.
	 * <br>
	 * a sibling ExecutorService would be prefered too, which will be used to
	 * execute {@link ISqlAuditor} asynchronously.
	 */
	private ISqlAuditor sqlAuditor;
	private ExecutorService sqlAuditorExecutor;

	public SpringSqlSessionTemplate(SqlSessionFactory sqlSessionFactory) {
		super(sqlSessionFactory);
	}

	public SpringSqlSessionTemplate(SqlSessionFactory sqlSessionFactory, ExecutorType executorType) {
		super(sqlSessionFactory, executorType);
	}

	public SpringSqlSessionTemplate(SqlSessionFactory sqlSessionFactory, ExecutorType executorType,
			PersistenceExceptionTranslator exceptionTranslator) {
		super(sqlSessionFactory, executorType, exceptionTranslator);
	}

	protected SortedMap<String, SqlSessionFactory> lookupDataSourcesByRouter(final String statementName,
			final Object parameterObject) {
		SortedMap<String, SqlSessionFactory> resultMap = new TreeMap<String, SqlSessionFactory>();
		if (getRouter() != null && getCobarDataSourceService() != null) {
			List<String> dsSet = getRouter().doRoute(new IBatisRoutingFact(statementName, parameterObject))
					.getResourceIdentities();
			if (CollectionUtils.isNotEmpty(dsSet)) {
				Collections.sort(dsSet);
				for (String dsName : dsSet) {
					resultMap.put(dsName, getCobarDataSourceService().getSessionFactorys().get(dsName));
				}
			}
		}
		return resultMap;
	}

	@Override
	public <T> T selectOne(String statement) {
		return this.selectOne(statement, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T selectOne(final String statement, final Object parameter) {
		if (isPartitioningBehaviorEnabled()) {
			SortedMap<String, SqlSessionFactory> dsMap = lookupDataSourcesByRouter(statement, parameter);
			if (!MapUtils.isEmpty(dsMap)) {
				SqlSessionCallback<T> action = new SqlSessionCallback<T>() {
					public T doInSqlSession(SqlSession executor) throws SQLException {
						return executor.selectOne(statement, parameter);
					}
				};
				if (dsMap.size() == 1) {
					return executeWith(dsMap.get(dsMap.firstKey()), action);
				} else {
					return ((T) executeInConcurrency2(action, dsMap).iterator().next());
				}
			}
		}
		return super.selectOne(statement, parameter);
	}

	@Override
	public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
		return this.selectMap(statement, null, mapKey, null);
	}

	@Override
	public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
		return this.selectMap(statement, parameter, mapKey, null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <K, V> Map<K, V> selectMap(final String statement, final Object parameter, final String mapKey,
			final RowBounds rowBounds) {
		if (isPartitioningBehaviorEnabled()) {
			SortedMap<String, SqlSessionFactory> dsMap = lookupDataSourcesByRouter(statement, parameter);
			if (!MapUtils.isEmpty(dsMap)) {
				SqlSessionCallback<Map> action = null;
				if (rowBounds == null) {
					action = new SqlSessionCallback<Map>() {
						public Map doInSqlSession(SqlSession executor) throws SQLException {
							return executor.selectMap(statement, parameter, mapKey);
						}
					};
				} else {
					action = new SqlSessionCallback<Map>() {
						public Map doInSqlSession(SqlSession executor) throws SQLException {
							return executor.selectMap(statement, parameter, mapKey, rowBounds);
						}
					};
				}

				if (dsMap.size() == 1) {
					return executeWith(dsMap.get(dsMap.firstKey()), action);
				} else {
					Map result = new HashMap();
					for (Object count : executeInConcurrency(action, dsMap)) {
						result.putAll((Map) count);
					}
					return result;
				}

			}
		}
		return super.selectMap(statement, parameter, mapKey, rowBounds);
	}

	@Override
	public <E> List<E> selectList(String statement) {
		return this.selectList(statement, null, null);
	}

	@Override
	public <E> List<E> selectList(String statement, Object parameter) {
		return this.selectList(statement, parameter, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E> List<E> selectList(final String statement, final Object parameter, final RowBounds rowBounds) {
		if (isPartitioningBehaviorEnabled()) {
			SortedMap<String, SqlSessionFactory> dsMap = lookupDataSourcesByRouter(statement, parameter);
			if (!MapUtils.isEmpty(dsMap)) {
				SqlSessionCallback<List<E>> action = null;
				if (rowBounds == null) {
					action = new SqlSessionCallback<List<E>>() {
						public List<E> doInSqlSession(SqlSession executor) throws SQLException {
							return executor.selectList(statement, parameter);
						}
					};
				} else {
					action = new SqlSessionCallback<List<E>>() {

						public List<E> doInSqlSession(SqlSession executor) throws SQLException {
							return executor.selectList(statement, parameter, rowBounds);
						}
					};
				}

				if (dsMap.size() == 1) {
					return executeWith(dsMap.get(dsMap.firstKey()), action);
				} else {

					List<E> result = new ArrayList<E>();
					for (Object count : executeInConcurrency(action, dsMap)) {
						result.addAll((List<E>) count);
					}
					return result;
				}

			}
		}
		if (rowBounds != null) {
			return super.selectList(statement, parameter, rowBounds);
		} else {
			return super.selectList(statement, parameter);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void select(String statement, ResultHandler handler) {
		this.select(statement, null, null, handler);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void select(String statement, Object parameter, ResultHandler handler) {
		this.select(statement, parameter, null, handler);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void select(final String statement, final Object parameter, final RowBounds rowBounds,
			final ResultHandler handler) {
		if (isPartitioningBehaviorEnabled()) {
			SortedMap<String, SqlSessionFactory> dsMap = lookupDataSourcesByRouter(statement, parameter);
			if (!MapUtils.isEmpty(dsMap)) {
				SqlSessionCallback action = null;
				if (rowBounds == null) {
					action = new SqlSessionCallback() {
						public Object doInSqlSession(SqlSession executor) throws SQLException {
							executor.select(statement, parameter, handler);
							return null;
						}
					};
				} else {
					action = new SqlSessionCallback() {

						public Object doInSqlSession(SqlSession executor) throws SQLException {
							executor.select(statement, parameter, rowBounds, handler);
							return null;
						}
					};
				}

				if (dsMap.size() == 1) {
					executeWith(dsMap.get(dsMap.firstKey()), action);
				} else {

					executeInConcurrency(action, dsMap);
				}

			}
		}
		super.select(statement, parameter, rowBounds, handler);
	}

	@Override
	public int delete(String statement) {
		return this.delete(statement, null);
	}

	@Override
	public int delete(final String statement, final Object parameter) {
		if (isPartitioningBehaviorEnabled()) {
			SortedMap<String, SqlSessionFactory> dsMap = lookupDataSourcesByRouter(statement, parameter);
			if (!MapUtils.isEmpty(dsMap) && !MapUtils.isEmpty(dsMap)) {
				SqlSessionCallback<Integer> action = new SqlSessionCallback<Integer>() {
					public Integer doInSqlSession(SqlSession executor) throws SQLException {
						return executor.delete(statement, parameter);
					}
				};

				if (dsMap.size() == 1) {
					return (Integer) executeWith(dsMap.get(dsMap.firstKey()), action);
				} else {
					int result = 0;
					for (Object count : executeInConcurrency(action, dsMap)) {
						result += (Integer) count;
					}
					return result;
				}
			}
		}
		return super.delete(statement, parameter);
	}

	@Override
	public int update(String statement) {
		return this.update(statement, null);
	}

	@Override
	public int update(final String statement, final Object parameter) {
		if (isPartitioningBehaviorEnabled()) {
			SortedMap<String, SqlSessionFactory> resultDataSources = lookupDataSourcesByRouter(statement, parameter);
			if (resultDataSources != null && !MapUtils.isEmpty(resultDataSources)) {
				SqlSessionCallback<Integer> action = new SqlSessionCallback<Integer>() {
					public Integer doInSqlSession(SqlSession executor) throws SQLException {
						return executor.update(statement, parameter);
					}
				};
				if (resultDataSources.size() == 1) {
					return (Integer) executeWith(resultDataSources.values().iterator().next(), action);
				} else {
					int result = 0;
					for (Object count : executeInConcurrency(action, resultDataSources)) {
						result += (Integer) count;
					}
					return result;
				}
			}
		}
		return super.update(statement, parameter);
	}

	@Override
	public int insert(String statement) {
		return this.insert(statement, null);
	}

	@Override
	public int insert(final String statement, final Object parameter) {
		if (isPartitioningBehaviorEnabled()) {
			SortedMap<String, SqlSessionFactory> resultDataSources = lookupDataSourcesByRouter(statement, parameter);
			if (resultDataSources != null && !MapUtils.isEmpty(resultDataSources)) {
				SqlSessionCallback<Integer> action = new SqlSessionCallback<Integer>() {
					public Integer doInSqlSession(SqlSession executor) throws SQLException {
						return executor.insert(statement, parameter);
					}
				};
				if (resultDataSources.size() == 1) {
					return (Integer) executeWith(resultDataSources.values().iterator().next(), action);
				} else {
					int result = 0;
					for (Object count : executeInConcurrency(action, resultDataSources)) {
						result += (Integer) count;
					}
					return result;
				}
			} else {
				logger.info("没有匹配的分区，使用默认分区");
			}
		} else {
			logger.info("不开启分区功能");
		}
		return super.insert(statement, parameter);
	}

	protected <T> T executeWith(SqlSession session, SqlSessionCallback<T> action) {
		try {
			return action.doInSqlSession(session);
		} catch (SQLException ex) {
			throw new SQLErrorCodeSQLExceptionTranslator().translate("SqlMapClient operation", null, ex);
		}
	}

	protected <T> T executeWith(SqlSessionFactory sqlSessionFactory, SqlSessionCallback<T> action) {
		SqlSession session = sqlSessionFactory.openSession();
		try {
			return action.doInSqlSession(session);
		} catch (SQLException ex) {
			throw new SQLErrorCodeSQLExceptionTranslator().translate("SqlMapClient operation", null, ex);
		} catch (Throwable t) {
			throw new UncategorizedCobarClientException("unknown excepton when performing data access operation.", t);
			// Processing finished - potentially session still to be closed.
		} finally {
			session.close();
		}
	}

	@SuppressWarnings("rawtypes")
	public List<Object> executeInConcurrency2(SqlSessionCallback action, SortedMap<String, SqlSessionFactory> dsMap) {
		List<ConcurrentRequest> requests = new ArrayList<ConcurrentRequest>();

		for (Map.Entry<String, SqlSessionFactory> entry : dsMap.entrySet()) {
			ConcurrentRequest request = new ConcurrentRequest();
			request.setAction(action);
			request.setSessionFactory(entry.getValue());
			request.setExecutor(getDataSourceSpecificExecutors().get(entry.getKey()));
			request.setSessionFactory(entry.getValue());
			requests.add(request);
		}

		List<Object> results = getConcurrentRequestProcessor().process(requests);
		return results;
	}

	protected <T> T executeWith(DataSource dataSource, SqlSessionCallback<T> action) {
		SqlSession session = null;
		try {
			Connection springCon = null;
			boolean transactionAware = (dataSource instanceof TransactionAwareDataSourceProxy);
			// Obtain JDBC Connection to operate on...
			try {
				springCon = (transactionAware ? dataSource.getConnection()
						: DataSourceUtils.doGetConnection(dataSource));
				session = this.getSqlSessionFactory().openSession(springCon);
			} catch (SQLException ex) {
				throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", ex);
			}

			try {
				return action.doInSqlSession(session);
			} catch (SQLException ex) {
				throw new SQLErrorCodeSQLExceptionTranslator().translate("SqlMapClient operation", null, ex);
			} catch (Throwable t) {
				throw new UncategorizedCobarClientException("unknown excepton when performing data access operation.",
						t);
			} finally {
				try {
					if (springCon != null) {
						if (transactionAware) {
							springCon.close();
						} else {
							DataSourceUtils.doReleaseConnection(springCon, dataSource);
						}
					}
				} catch (Throwable ex) {
					logger.debug("Could not close JDBC Connection", ex);
				}
			}
			// Processing finished - potentially session still to be closed.
		} finally {
			session.close();
		}
	}

	@SuppressWarnings("rawtypes")
	public List<Object> executeInConcurrency(SqlSessionCallback action, SortedMap<String, SqlSessionFactory> dsMap) {
		List<ConcurrentRequest> requests = new ArrayList<ConcurrentRequest>();

		for (Map.Entry<String, SqlSessionFactory> entry : dsMap.entrySet()) {
			ConcurrentRequest request = new ConcurrentRequest();
			request.setAction(action);
			request.setSessionFactory(entry.getValue());
			request.setExecutor(getDataSourceSpecificExecutors().get(entry.getKey()));
			requests.add(request);
		}

		List<Object> results = getConcurrentRequestProcessor().process(requests);
		return results;
	}

	@Override
	public void afterPropertiesSet() {
		setupDefaultExecutorServicesIfNecessary();
		setUpDefaultSqlAuditorExecutorIfNecessary();
		if (getConcurrentRequestProcessor() == null) {
			setConcurrentRequestProcessor(new DefaultConcurrentRequestProcessor(getSqlSessionFactory()));
		}
	}

	/**
	 * if a SqlAuditor is injected and a sqlAuditorExecutor is NOT provided
	 * together, we need to setup a sqlAuditorExecutor so that the SQL auditing
	 * actions can be performed asynchronously. <br>
	 * otherwise, the data access process may be blocked by auditing SQL.<br>
	 * Although an external ExecutorService can be injected for use, normally,
	 * it's not so necessary.<br>
	 * Most of the time, you should inject an proper {@link ISqlAuditor} which
	 * will do SQL auditing in a asynchronous way.<br>
	 */
	private void setUpDefaultSqlAuditorExecutorIfNecessary() {
		if (sqlAuditor != null && sqlAuditorExecutor == null) {
			sqlAuditorExecutor = createCustomExecutorService(1, "setUpDefaultSqlAuditorExecutorIfNecessary");
			// 1. register executor for disposing later explicitly
			internalExecutorServiceRegistry.add(sqlAuditorExecutor);
			// 2. dispose executor implicitly
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					if (sqlAuditorExecutor == null) {
						return;
					}
					try {
						sqlAuditorExecutor.shutdown();
						sqlAuditorExecutor.awaitTermination(5, TimeUnit.MINUTES);
					} catch (InterruptedException e) {
						logger.warn("interrupted when shuting down the query executor:\n{}", e);
					}
				}
			});
		}
	}

	/**
	 * If more than one data sources are involved in a data access request, we
	 * need a collection of executors to execute the request on these data
	 * sources in parallel.<br>
	 * But in case the users forget to inject a collection of executors for this
	 * purpose, we need to setup a default one.<br>
	 */
	private void setupDefaultExecutorServicesIfNecessary() {
		if (isPartitioningBehaviorEnabled()) {

			if (MapUtils.isEmpty(getDataSourceSpecificExecutors())) {

				Set<CobarSessionFactoryDescriptor> dataSourceDescriptors = getCobarDataSourceService()
						.getSessionFactoryDescriptors();
				for (CobarSessionFactoryDescriptor descriptor : dataSourceDescriptors) {
					ExecutorService executor = createExecutorForSpecificDataSource(descriptor);
					getDataSourceSpecificExecutors().put(descriptor.getIdentity(), executor);
				}
			}
		}
	}

	private ExecutorService createExecutorForSpecificDataSource(CobarSessionFactoryDescriptor descriptor) {
		final String identity = descriptor.getIdentity();
		final ExecutorService executor = createCustomExecutorService(descriptor.getPoolSize(),
				"createExecutorForSpecificDataSource-" + identity + " data source");
		// 1. register executor for disposing explicitly
		internalExecutorServiceRegistry.add(executor);
		// 2. dispose executor implicitly
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (executor == null) {
					return;
				}

				try {
					executor.shutdown();
					executor.awaitTermination(5, TimeUnit.MINUTES);
				} catch (InterruptedException e) {
					logger.warn("interrupted when shuting down the query executor:\n{}", e);
				}
			}
		});
		return executor;
	}

	private ExecutorService createCustomExecutorService(int poolSize, final String method) {
		int coreSize = Runtime.getRuntime().availableProcessors();
		if (poolSize < coreSize) {
			coreSize = poolSize;
		}
		ThreadFactory tf = new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "thread created at CobarSqlMapClientTemplate method [" + method + "]");
				t.setDaemon(true);
				return t;
			}
		};
		BlockingQueue<Runnable> queueToUse = new LinkedBlockingQueue<Runnable>(coreSize);
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(coreSize, poolSize, 60, TimeUnit.SECONDS, queueToUse,
				tf, new ThreadPoolExecutor.CallerRunsPolicy());

		return executor;
	}

	protected void auditSqlIfNecessary(final String statementName, final Object parameterObject) {
		if (getSqlAuditor() != null) {
			getSqlAuditorExecutor().execute(new Runnable() {
				public void run() {
					getSqlAuditor().audit(statementName, getSqlByStatementName(statementName, parameterObject),
							parameterObject);
				}
			});
		}
	}

	protected String getSqlByStatementName(String statementName, Object parameterObject) {
		SqlSource sqlSource = getSqlSessionFactory().openSession().getConfiguration().getMappedStatement(statementName)
				.getSqlSource();
		if (sqlSource instanceof StaticSqlSource) {
			return sqlSource.getBoundSql(statementName).getSql();
		} else {
			logger.info("dynamic sql can only return sql id.");
			return statementName;
		}
	}

	@Override
	public void destroy() throws Exception {
		if (CollectionUtils.isNotEmpty(internalExecutorServiceRegistry)) {
			logger.info("shutdown executors of CobarSqlMapClientTemplate...");
			for (ExecutorService executor : internalExecutorServiceRegistry) {
				if (executor != null) {
					try {
						executor.shutdown();
						executor.awaitTermination(5, TimeUnit.MINUTES);
						executor = null;
					} catch (InterruptedException e) {
						logger.warn("interrupted when shuting down the query executor:\n{}", e);
					}
				}
			}
			getDataSourceSpecificExecutors().clear();
			logger.info("all of the executor services in CobarSqlMapClientTemplate are disposed.");
		}
	}

	/**
	 * 是否采取分区
	 * 
	 * @return
	 */
	protected boolean isPartitioningBehaviorEnabled() {
		return ((router != null) && (getCobarDataSourceService() != null));
	}

	public ICobarSessionFactoryService getCobarDataSourceService() {
		return cobarDataSourceService;
	}

	public void setCobarDataSourceService(ICobarSessionFactoryService cobarDataSourceService) {
		this.cobarDataSourceService = cobarDataSourceService;
	}

	public ICobarRouter<IBatisRoutingFact> getRouter() {
		return router;
	}

	public void setRouter(ICobarRouter<IBatisRoutingFact> router) {
		this.router = router;
	}

	public IConcurrentRequestProcessor getConcurrentRequestProcessor() {
		return concurrentRequestProcessor;
	}

	public void setConcurrentRequestProcessor(IConcurrentRequestProcessor concurrentRequestProcessor) {
		this.concurrentRequestProcessor = concurrentRequestProcessor;
	}

	public Map<String, ExecutorService> getDataSourceSpecificExecutors() {
		return dataSourceSpecificExecutors;
	}

	public void setDataSourceSpecificExecutors(Map<String, ExecutorService> dataSourceSpecificExecutors) {
		this.dataSourceSpecificExecutors = dataSourceSpecificExecutors;
	}

	public ISqlAuditor getSqlAuditor() {
		return sqlAuditor;
	}

	public void setSqlAuditorExecutor(ExecutorService sqlAuditorExecutor) {
		this.sqlAuditorExecutor = sqlAuditorExecutor;
	}

	public ExecutorService getSqlAuditorExecutor() {
		return sqlAuditorExecutor;
	}

}
