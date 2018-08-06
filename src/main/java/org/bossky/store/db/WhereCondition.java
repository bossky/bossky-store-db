package org.bossky.store.db;

/**
 * where语句
 * 
 * @author daibo
 *
 */
public class WhereCondition {

	protected String where;

	public WhereCondition(String where) {
		this.where = where;
	}

	public String getWhere() {
		return where;
	}
}
