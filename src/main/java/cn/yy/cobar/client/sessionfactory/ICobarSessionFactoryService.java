package cn.yy.cobar.client.sessionfactory;

import java.util.Map;
import java.util.Set;

import org.apache.ibatis.session.SqlSessionFactory;

public interface ICobarSessionFactoryService {
	Map<String, SqlSessionFactory> getSessionFactorys();

	Set<CobarSessionFactoryDescriptor> getSessionFactoryDescriptors();
}
