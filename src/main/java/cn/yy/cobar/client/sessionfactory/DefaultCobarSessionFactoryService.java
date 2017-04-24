package cn.yy.cobar.client.sessionfactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.InitializingBean;

import cn.yy.cobar.client.support.utils.CollectionUtils;

public class DefaultCobarSessionFactoryService implements ICobarSessionFactoryService, InitializingBean {
	private Set<CobarSessionFactoryDescriptor> sessionFactoryDescriptors = new HashSet<CobarSessionFactoryDescriptor>();
	private Map<String, SqlSessionFactory> sessionFactorys = new HashMap<String, SqlSessionFactory>();

	public void afterPropertiesSet() throws Exception {
		if (CollectionUtils.isEmpty(sessionFactoryDescriptors)) {
			return;
		}

		for (CobarSessionFactoryDescriptor descriptor : getSessionFactoryDescriptors()) {
			Validate.notEmpty(descriptor.getIdentity());
			Validate.notNull(descriptor.getTargetSqlSessionFactory());
			SqlSessionFactory dataSourceToUse = descriptor.getTargetSqlSessionFactory();
			sessionFactorys.put(descriptor.getIdentity(), dataSourceToUse);
		}
	}

	public Set<CobarSessionFactoryDescriptor> getSessionFactoryDescriptors() {
		return sessionFactoryDescriptors;
	}

	public Map<String, SqlSessionFactory> getSessionFactorys() {
		return sessionFactorys;
	}

	public void setSessionFactoryDescriptors(Set<CobarSessionFactoryDescriptor> sessionFactoryDescriptors) {
		this.sessionFactoryDescriptors = sessionFactoryDescriptors;
	}

}
