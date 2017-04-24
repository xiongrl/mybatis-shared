/**
 * Copyright 1999-2011 Alibaba Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.yy.cobar.client.support.execution;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.Validate;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

import cn.yy.cobar.client.spring.SqlSessionCallback;
import cn.yy.cobar.client.support.utils.CollectionUtils;

public class DefaultConcurrentRequestProcessor implements IConcurrentRequestProcessor {

	private SqlSessionFactory sqlMapClient;

	public DefaultConcurrentRequestProcessor() {
	}

	public DefaultConcurrentRequestProcessor(SqlSessionFactory sqlMapClient) {
		this.sqlMapClient = sqlMapClient;
	}

	public List<Object> process(List<ConcurrentRequest> requests) {
		List<Object> resultList = new ArrayList<Object>();

		if (CollectionUtils.isEmpty(requests))
			return resultList;

		List<RequestDepository> requestsDepo = fetchConnectionsAndDepositForLaterUse(requests);
		final CountDownLatch latch = new CountDownLatch(requestsDepo.size());
		List<Future<Object>> futures = new ArrayList<Future<Object>>();
		try {

			for (RequestDepository rdepo : requestsDepo) {
				ConcurrentRequest request = rdepo.getOriginalRequest();
				final SqlSessionCallback<?> action = request.getAction();
				final SqlSession sqlSession = rdepo.getSqlSession();
				futures.add(request.getExecutor().submit(new Callable<Object>() {
					public Object call() throws Exception {
						try {
							return executeWith(sqlSession, action);
						} finally {
							latch.countDown();
						}
					}
				}));
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new ConcurrencyFailureException("interrupted when processing data access request in concurrency",
						e);
			}

		} finally {
			for (RequestDepository depo : requestsDepo) {
				depo.getSqlSession().close();
			}
		}

		fillResultListWithFutureResults(futures, resultList);

		return resultList;
	}

	protected Object executeWith(SqlSession session, SqlSessionCallback<?> action) {
		// SqlSession session = null;
		try {
			// session = getSqlMapClient().openSession(connection);
			try {
				return action.doInSqlSession(session);
			} catch (SQLException ex) {
				throw new SQLErrorCodeSQLExceptionTranslator().translate("SqlSession operation", null, ex);
			}
		} finally {
			if (session != null) {
				session.close();
			}
		}
	}

	private void fillResultListWithFutureResults(List<Future<Object>> futures, List<Object> resultList) {
		for (Future<Object> future : futures) {
			try {
				resultList.add(future.get());
			} catch (InterruptedException e) {
				throw new ConcurrencyFailureException("interrupted when processing data access request in concurrency",
						e);
			} catch (ExecutionException e) {
				throw new ConcurrencyFailureException("something goes wrong in processing", e);
			}
		}
	}

	private List<RequestDepository> fetchConnectionsAndDepositForLaterUse(List<ConcurrentRequest> requests) {
		List<RequestDepository> depos = new ArrayList<RequestDepository>();
		for (ConcurrentRequest request : requests) {

			RequestDepository depo = new RequestDepository();
			depo.setOriginalRequest(request);
			depo.setSqlSession(request.getSessionFactory().openSession());
			depos.add(depo);
		}

		return depos;
	}

	public void setSqlMapClient(SqlSessionFactory sqlMapClient) {
		Validate.notNull(sqlMapClient);
		this.sqlMapClient = sqlMapClient;
	}

	public SqlSessionFactory getSqlMapClient() {
		return sqlMapClient;
	}

}
