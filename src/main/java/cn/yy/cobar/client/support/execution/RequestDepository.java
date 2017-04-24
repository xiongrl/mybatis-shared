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

import org.apache.ibatis.session.SqlSession;

/**
 * temporary status depository for request processing.<br>
 * 
 * @author fujohnwang
 * @since 1.0
 */
public class RequestDepository {
	private ConcurrentRequest originalRequest;
	private SqlSession sqlSession;

	public ConcurrentRequest getOriginalRequest() {
		return originalRequest;
	}

	public void setOriginalRequest(ConcurrentRequest originalRequest) {
		this.originalRequest = originalRequest;
	}

	public SqlSession getSqlSession() {
		return sqlSession;
	}

	public void setSqlSession(SqlSession sqlSession) {
		this.sqlSession = sqlSession;
	}

}
