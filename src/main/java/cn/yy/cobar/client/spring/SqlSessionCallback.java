package cn.yy.cobar.client.spring;

import java.sql.SQLException;

import org.apache.ibatis.session.SqlSession;

public interface SqlSessionCallback<T> {

	T doInSqlSession(SqlSession sqlSession) throws SQLException;

}
