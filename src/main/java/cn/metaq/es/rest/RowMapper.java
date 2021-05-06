package cn.metaq.es.rest;

import java.util.Map;

public interface RowMapper<T> {

    Map mapRow(T var1);
}
