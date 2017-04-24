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
package cn.yy.cobar.client.sessionfactory;

import org.apache.ibatis.session.SqlSessionFactory;

/**
 * {@link CobarSessionFactoryDescriptor} describe a data base deployment
 * structure with 2 databases as HA group.<br>
 * it looks like:<br>
 * 
 * <pre>
 *                  Client
 *                    /\
 *                  /    \
 *         Active DB <-> Standby DB
 * </pre>
 * 
 * {@link #targetSqlSessionFactory} should be the reference to the current
 * active database, while {@link #standbySqlSessionFactory} should be the
 * standby database. <br>
 * for both {@link #targetSqlSessionFactory} and
 * {@link #standbySqlSessionFactory}, each one should have a sibling data source
 * that connect to the same target database. <br>
 * as to the reason why do so, that's :
 * <ol>
 * <li>these sibling SqlSessionFactorys will be used when do
 * database-status-detecting.(if we fetch connection from target data source,
 * when it's full, the deteting behavior can't be performed.)</li>
 * <li>if the {@link #targetSqlSessionFactory} and
 * {@link #standbySqlSessionFactory} are SqlSessionFactory implementations
 * configured in local application container, we can fetch necessary information
 * via reflection to create connection to target database independently, but if
 * they are fetched from JNDI, we can't, so explicitly declaring sibling data
 * sources is necessary in this situation.</li>
 * </ol>
 * 
 * @author fujohnwang
 * @since 1.0
 */
public class CobarSessionFactoryDescriptor {
	/**
	 * the identity of to-be-exposed SqlSessionFactory.
	 */
	private String identity;
	/**
	 * active data source
	 */
	private SqlSessionFactory targetSqlSessionFactory;
	/**
	 * we will initialize proper thread pools which stand in front of data
	 * sources as per connection pool size. <br>
	 * usually, they will have same number of objects.<br>
	 * you have to set a proper size for this attribute as per your data source
	 * attributes. In case you forget it, we set a default value with
	 * "number of CPU" * 5.
	 */
	private int poolSize = Runtime.getRuntime().availableProcessors() * 5;

	public String getIdentity() {
		return identity;
	}

	public void setIdentity(String identity) {
		this.identity = identity;
	}

	public SqlSessionFactory getTargetSqlSessionFactory() {
		return targetSqlSessionFactory;
	}

	public void setTargetSqlSessionFactory(SqlSessionFactory targetSqlSessionFactory) {
		this.targetSqlSessionFactory = targetSqlSessionFactory;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public int getPoolSize() {
		return poolSize;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((identity == null) ? 0 : identity.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CobarSessionFactoryDescriptor other = (CobarSessionFactoryDescriptor) obj;
		if (identity == null) {
			if (other.identity != null)
				return false;
		} else if (!identity.equals(other.identity))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CobarSqlSessionFactoryDescriptor [identity=" + identity + ", poolSize=" + poolSize + "]";
	}

}
