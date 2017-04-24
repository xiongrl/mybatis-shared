package cn.yy.cobar.client.spring;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Autowired;

public class SpringSqlSessionDaoSupport implements SqlSessionOperations {

	@Autowired
	private SpringSqlSessionTemplate springSqlSessionTemplate;

	@Override
	public <T> T selectOne(String statement) {
		return getSpringSqlSessionTemplate().selectOne(statement);
	}

	@Override
	public <T> T selectOne(String statement, Object parameter) {
		return getSpringSqlSessionTemplate().selectOne(statement, parameter);
	}

	@Override
	public <E> List<E> selectList(String statement) {
		return getSpringSqlSessionTemplate().selectList(statement);
	}

	@Override
	public <E> List<E> selectList(String statement, Object parameter) {
		return getSpringSqlSessionTemplate().selectList(statement, parameter);
	}

	@Override
	public <E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds) {
		return getSpringSqlSessionTemplate().selectList(statement, parameter, rowBounds);
	}

	@Override
	public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
		return getSpringSqlSessionTemplate().selectMap(statement, mapKey);
	}

	@Override
	public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
		return getSpringSqlSessionTemplate().selectMap(statement, parameter, mapKey);
	}

	@Override
	public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey, RowBounds rowBounds) {
		return getSpringSqlSessionTemplate().selectMap(statement, parameter, mapKey, rowBounds);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void select(String statement, Object parameter, ResultHandler handler) {
		getSpringSqlSessionTemplate().select(statement, parameter, handler);

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void select(String statement, ResultHandler handler) {
		getSpringSqlSessionTemplate().select(statement, handler);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void select(String statement, Object parameter, RowBounds rowBounds, ResultHandler handler) {
		getSpringSqlSessionTemplate().select(statement, parameter, rowBounds, handler);
	}

	@Override
	public int insert(String statement) {
		return getSpringSqlSessionTemplate().insert(statement);
	}

	@Override
	public int insert(String statement, Object parameter) {
		return getSpringSqlSessionTemplate().insert(statement, parameter);
	}

	@Override
	public int update(String statement) {
		return getSpringSqlSessionTemplate().update(statement);
	}

	@Override
	public int update(String statement, Object parameter) {
		return getSpringSqlSessionTemplate().update(statement, parameter);
	}

	@Override
	public int delete(String statement) {
		return getSpringSqlSessionTemplate().delete(statement);
	}

	@Override
	public int delete(String statement, Object parameter) {
		return getSpringSqlSessionTemplate().delete(statement, parameter);
	}

	public SpringSqlSessionTemplate getSpringSqlSessionTemplate() {
		return springSqlSessionTemplate;
	}

	public void setSpringSqlSessionTemplate(SpringSqlSessionTemplate springSqlSessionTemplate) {
		this.springSqlSessionTemplate = springSqlSessionTemplate;
	}

}
