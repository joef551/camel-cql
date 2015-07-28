/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.metis.cassandra;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;

import org.apache.camel.Message;
import org.metis.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;

import org.metis.cassandra.Client.Method;

import static org.metis.utils.Constants.*;

/**
 * Object that encapsulates or represents a CQL statement. This is a
 * thread-safe/re-entrant singleton bean!
 */
public class CqlStmnt implements InitializingBean, BeanNameAware {

	public static final Logger LOG = LoggerFactory.getLogger(CqlStmnt.class);

	// the injected statement
	private String statement;

	// set of properties used to mimic com.datastax.driver.core.Statement
	private int fetchSize = -1;
	private boolean pagingState;
	private boolean tracing;
	private long defaultTimestamp = -1L;
	private boolean idempotent;
	private Boolean myIdempotent = null;
	private RetryPolicy retryPolicy;
	private ConsistencyLevel consistencyLevel;
	private ConsistencyLevel serialConsistencyLevel;

	// this bean's id
	private String beanName;

	// this is the string version of the prepared statement for this CQL
	// statement; i.e., assuming this is a prepared statement.
	private String preparedStr = "";

	// this list contains 'all' the tokens that comprise this CQL statement;
	// in other words, it contains both regular and key/parameterized tokens
	private ArrayList<CqlToken> tokens = new ArrayList<CqlToken>();

	// this map contains only the parameterized key tokens; e.g., `int:id` is a
	// parameterized token.
	private Map<String, CqlToken> keyTokens = new HashMap<String, CqlToken>();

	// the type of CQL statement that this statement represents; e.g., select,
	// insert, update, etc.
	private Method cqlStmntType;

	// marks this statement as a "SELECT JSON ..." statement
	private boolean isJsonSelect;

	// A Map of bound and simple statement pools, where each pool pertains to a
	// particular Cassandra session.
	private Map<Session, CqlStmntPool> stmntPool = new ConcurrentHashMap<Session, CqlStmntPool>();

	// governs the statement stack sizes in each CqlStmntPool
	private int stackSize = 25;

	public CqlStmnt() {
	}

	public CqlStmnt(String statement) {
		setStatement(statement);
	}

	/**
	 * Get the CqlStmntType for this statement.
	 * 
	 * @return
	 */
	public Method getCqlStmntType() {
		return cqlStmntType;
	}

	/**
	 * Set the CqlStmntType for this statement
	 * 
	 * @param cqlStmntType
	 */
	public void setCqlStmntType(Method cqlStmntType) {
		this.cqlStmntType = cqlStmntType;
	}

	/**
	 * Get the prepared string version of this statement.
	 * 
	 * @return
	 */
	public String getPreparedStr() {
		return preparedStr;
	}

	public void setPreparedStr(String preparedStr) {
		this.preparedStr = preparedStr;
	}

	/**
	 * Returns true if this statement is prepared.
	 * 
	 * @return
	 */
	public boolean isPrepared() {
		return !getPreparedStr().isEmpty();
	}

	/**
	 * Return the list of all tokens that comprise this CQL statement object.
	 * 
	 * @return
	 */
	public ArrayList<CqlToken> getTokens() {
		return tokens;
	}

	public void addToken(CqlToken token) {
		getTokens().add(token);
	}

	/**
	 * Return the key fields/tokens (if any) for this statement
	 * 
	 * @return
	 */
	public Map<String, CqlToken> getKeyTokens() {
		return keyTokens;
	}

	/**
	 * Returns the number of tokens that are of type 'key'
	 * 
	 * @return
	 */
	public int getNumKeyTokens() {
		return keyTokens.size();
	}

	public boolean isSelect() {
		return getCqlStmntType() == Method.SELECT;
	}

	public boolean isDelete() {
		return getCqlStmntType() == Method.DELETE;
	}

	public boolean isInsert() {
		return getCqlStmntType() == Method.INSERT;
	}

	public boolean isUpdate() {
		return getCqlStmntType() == Method.UPDATE;
	}

	/**
	 * Called to determine if the given set of keys (input param names) matches
	 * those in this statement. If there are no keys given and this statement is
	 * not a prepared statement, then return true because this statement has no
	 * keys.
	 * 
	 * @param keys
	 * @return
	 */
	public boolean isMatch(Set<String> keys) {

		if (keys == null || keys.isEmpty()) {
			LOG.trace("isMatch: given key set is null or empty");
			if (!isPrepared()) {
				LOG.trace("isMatch: returning true because this stmt is not "
						+ "prepared and no keys were provided");
				return true;
			}
			return false;
		} else {
			// every key in this statement must match every given key
			if (keys.size() != getNumKeyTokens()) {
				LOG.trace(
						"isMatch: returning false because key set of size {} "
								+ "does not match my number of key tokens {}",
						keys.size(), getNumKeyTokens());
				return false;
			}
			for (String key : keys) {
				if (keyTokens.get(key) == null) {
					LOG.trace("isMatch: returning false because this given "
							+ "key does not match: " + key);
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Returns true if this statement is equal to the given one. Equality is
	 * strictly based on those key:value pairs that are for 'input' params. This
	 * method is indirectly called, via isEqual(List), by afterProperties in the
	 * Client to ensure there will be no mapping conflicts between the
	 * Spring-injected cql statements. That is, two CQL statements with the same
	 * set of input params.
	 * 
	 * @param stmt
	 * @return
	 */
	public boolean isEqual(CqlStmnt stmt) {

		// get a count of the number of input fields for this statement
		int it1Cnt = isPrepared() ? getKeyTokens().size() : 0;

		// do the same for the given statement
		int it2Cnt = stmt.isPrepared() ? stmt.getKeyTokens().size() : 0;

		// if the number of input fields don't match up
		// return false. if they're both equal to zero,
		// return true
		if (it1Cnt != it2Cnt) {
			return false;
		} else if (it1Cnt + it2Cnt == 0) {
			// if neither have any input params, then they're
			// considered equal; even if one is prepared and
			// the other one is not. if that were the case,
			// the prepared
			return true;
		}

		// iterate through this statement's key fields and compare them
		// against the given stmnt
		for (CqlToken token : getKeyTokens().values()) {
			if (stmt.getKeyTokens().get(token.getKey()) == null) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Returns true if this statement is equal to one in the given list.
	 * 
	 * @param stmts
	 * @return
	 */
	public boolean isEqual(List<CqlStmnt> stmts) {
		for (CqlStmnt stmt : stmts) {
			if (isEqual(stmt)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * From the given list of CQL statements, return the one that matches the
	 * given set of keys. The keys represent input params
	 * 
	 * <code>
	 * 1. If there are no keys (i.e., no input params) to work with, then the 
	 * first non-prepared CQL statement will be returned. Thus, only one 
	 * non-prepared CQL statement can or should be used  within a given list. 
	 * This takes highest precedence when no input params are given.
	 * 
	 * 
	 * 2. If there are keys, then the match will be performed off those keys. 
	 * 
	 * </code>
	 * 
	 * @param stmnts
	 * @param keys
	 * @return
	 */
	public static CqlStmnt getMatch(List<CqlStmnt> stmnts, Set<String> keys) {

		if (stmnts == null || stmnts.size() == 0) {
			LOG.warn("getMatch: null or empty list of statements "
					+ "was provided");
			return null;
		} else {
			LOG.debug("getMatch: {} statements are in provided list ",
					stmnts.size());

		}

		// if there are no keys, then follow step 1
		if (keys == null || keys.isEmpty()) {
			LOG.debug("getMatch: input params were not provided");
			// search for the first non-prepared statement
			for (CqlStmnt stmnt : stmnts) {
				if (!stmnt.isPrepared()) {
					LOG.debug("getMatch: returning this non-prepared statement: "
							+ stmnt.getStatement());
					return stmnt;
				}
			}
			LOG.warn("getMatch: returning null");
			return null;
		}

		// We have keys to work with so we'll work off the given keys
		LOG.trace("getMatch: these input params were provided: "
				+ keys.toString());
		for (CqlStmnt stmnt : stmnts) {
			LOG.trace("getMatch: checking against this statement {}",
					stmnt.getStatement());
			if (stmnt.isMatch(keys)) {
				LOG.debug("getMatch: returning this statement: "
						+ stmnt.getStatement());
				return stmnt;
			}
		}
		LOG.warn("getMatch: keys were provided, but no parameterized cql "
				+ "statement could be found");
		return null;
	}

	/**
	 * CqlStmnt factory used for creating a CqlStmnt object from the given CQL
	 * string.
	 * 
	 * @param wdch
	 * @param cql
	 * @param jdbcTemplate
	 * @return
	 * @throws IllegalArgumentException
	 * @throws Exception
	 */
	public void afterPropertiesSet() throws Exception {

		LOG.debug(getBeanName() + ":afterPropertiesSet: starting statement = ["
				+ getStatement() + "]");

		if (getStatement() == null || getStatement().isEmpty()) {
			throw new IllegalArgumentException(
					"getCQLTokens: cql string is null or empty");
		}

		// lets take out any extra white spaces from the injected statement
		String[] tokens = getStatement().trim().split(DELIM);
		if (tokens == null || tokens.length < 2) {
			if (tokens != null) {
				throw new IllegalArgumentException(
						"getCQLTokens: detected invalid token count of "
								+ tokens.length);
			} else {
				throw new IllegalArgumentException(
						"getCQLTokens: detected null tokens array");
			}
		}
		String cql2 = EMPTY_STR;
		for (int i = 0; i < tokens.length; i++) {
			cql2 += tokens[i].trim();
			if (i < tokens.length - 1) {
				cql2 += SPACE_STR;
			}
		}
		// set the new trimmed statement
		setStatement(cql2);

		// begin initial validation work
		if (UPDATE_STR.equalsIgnoreCase(tokens[0])) {
			valCql4Update(getStatement());
		} else if (DELETE_STR.equalsIgnoreCase(tokens[0])) {
			valCql4Delete(getStatement());
		} else if (SELECT_STR.equalsIgnoreCase(tokens[0])) {
			valCql4Select(getStatement());
		} else if (INSERT_STR.equalsIgnoreCase(tokens[0])) {
			valCql4Insert(getStatement());
		} else {
			throw new IllegalArgumentException("invalid CQL statement: "
					+ getStatement());
		}

		// remove spaces that may be embedded in the field tokens. for example:
		// select * from users where first like ` string : first `
		// will get transformed to:
		//
		// select * from users where first like `string:first`
		//
		// this is done to ensure that the entire field token is
		// treated as one token
		//
		// remove spaces will also ensure that any ` ... ` strings are
		// surrounded by spaces, thus also ensuring the entire string is
		// treated as one token
		setStatement(removeSpaces(getStatement()));

		LOG.debug(getBeanName() + ":afterPropertiesSet: final statement = ["
				+ getStatement() + "]");

		// split the final statement into tokens; they will be converted to
		// CqlTokens
		tokens = getStatement().trim().split(DELIM);

		// the list that will hold the resulting CqlTokens
		ArrayList<CqlToken> tList = new ArrayList<CqlToken>();

		// used for keeping track of the position that a field-token will take
		// up in a bound statement
		int pos = 0;

		// convert the string tokens to CqlTokens and tuck them into the token
		// list
		for (String token : tokens) {
			if (token.startsWith(BACK_QUOTE_STR)
					&& !token.endsWith(BACK_QUOTE_STR)) {
				throw new IllegalArgumentException(
						"this parameter token doesn't have a closing \"`\" character"
								+ token);
			} else if (!token.startsWith(BACK_QUOTE_STR)
					&& token.endsWith(BACK_QUOTE_STR)) {
				throw new IllegalArgumentException(
						"this parameter token has a starting \"`\" character, but not a closing one: "
								+ token);
			} else if (token.startsWith(BACK_QUOTE_STR)
					&& token.endsWith(BACK_QUOTE_STR)) {
				// extract the type:[collection-type]:key-name from the field
				// token strip out the leading and trailing '`' chars
				// tokenize based on the ':' delimiter
				String[] tks = token.substring(1, token.length() - 1).split(
						COLON_STR);
				// CqlType, Key, Position
				if (tks.length == 2) {
					tList.add(new CqlToken(tks[0].trim(), tks[1].trim(), pos));
				}
				// CqlType, Collection-Type, key, position
				// where CqlType specifies what type of collection (e.g., Set,
				// List, Map) and Collection-Type further qualifies it as a list
				// of Strings, list of Longs, etc.
				else if (tks.length == 3) {
					tList.add(new CqlToken(tks[0].trim(), tks[1].trim(), tks[2]
							.trim(), pos));
				} else {
					throw new IllegalArgumentException(
							"Invalid CQL statement - paramterized token ["
									+ token + "] is not valid");
				}
				pos++;
			} else {
				// not a field-token so just tuck it away
				tList.add(new CqlToken(token));
			}
		}
		// finish this statement's initialization based on the derived tokens
		init(tList);
		// return new CqlStmnt(cql, tList);
	}

	/**
	 * Called by the Client bean to execute this CQL statement with the given
	 * params.
	 * 
	 */
	public ResultSet execute(Map<String, Object> inParams, Message inMsg,
			Session session) {

		Map<String, Object> params = (inParams == null) ? new HashMap<String, Object>()
				: inParams;

		if (LOG.isDebugEnabled()) {
			LOG.debug(
					getBeanName() + ":execute: executing this statement: {} ",
					getStatement());
			LOG.debug(getBeanName()
					+ ":execute: executing this prepared statement: {} ",
					getPreparedStr());
			LOG.debug(getBeanName()
					+ ":execute: executing with this number {} of params",
					params.size());
		}

		if (session == null) {
			LOG.error("execute: session is null");
			return null;
		} else if (session.isClosed()) {
			LOG.error("execute: session is closed");
			stmntPool.remove(session);
			return null;
		}

		// grab the statement pool pertaining to the session.
		// if one does not exist, create one
		CqlStmntPool cqlStmntPool = null;
		synchronized (stmntPool) {
			cqlStmntPool = stmntPool.get(session);
			if (cqlStmntPool == null) {
				cqlStmntPool = new CqlStmntPool();
				stmntPool.put(session, cqlStmntPool);
			}
		}

		// if this CQL statement is a prepared statement, ensure that it has
		// been prepared for this session
		synchronized (this) {
			if (isPrepared() && cqlStmntPool.getPreparedStatement() == null) {
				cqlStmntPool.setPreparedStatement(session
						.prepare(getPreparedStr()));
			}
		}

		// grab some default info from Cassy session (if required)
		ConsistencyLevel cLevel = (getConsistencyLevel() != null) ? getConsistencyLevel()
				: session.getCluster().getConfiguration().getQueryOptions()
						.getConsistencyLevel();

		ConsistencyLevel sLevel = (getSerialConsistencyLevel() != null) ? getSerialConsistencyLevel()
				: session.getCluster().getConfiguration().getQueryOptions()
						.getSerialConsistencyLevel();

		int fetchSize = (getFetchSize() >= 0) ? getFetchSize() : session
				.getCluster().getConfiguration().getQueryOptions()
				.getFetchSize();		

		RetryPolicy retryPolicy = (getRetryPolicy() != null) ? getRetryPolicy()
				: session.getCluster().getConfiguration().getPolicies()
						.getRetryPolicy();

		boolean idempotent = (getMyIdempotent() != null) ? getMyIdempotent()
				: session.getCluster().getConfiguration().getQueryOptions()
						.getDefaultIdempotence();

		if (LOG.isDebugEnabled()) {
			LOG.debug(getBeanName() + ":execute: consistency level: {} ",
					cLevel.toString());
			LOG.debug(
					getBeanName() + ":execute: seriel consistency level: {} ",
					sLevel.toString());
			LOG.debug(getBeanName() + ":execute: fetchSize: {} ", fetchSize);
			LOG.debug(getBeanName() + ":execute: RetryPolcy : {} ",
					retryPolicy.toString());
			LOG.debug(getBeanName() + ":execute: pagingState : {} ",
					isPagingState());
			LOG.debug(getBeanName() + ":execute: idempotent : {} ", idempotent);
			LOG.debug(getBeanName() + ":execute: defaultTimestamp : {} ",
					getDefaultTimestamp());
		}

		// first, do some light validation work
		if (params.isEmpty() && isPrepared()) {
			LOG.error(getBeanName()
					+ ":execute: ERROR, params were not provided "
					+ "for this prepared statement {}", getPreparedStr());
			return null;
		} else if (params.size() > 0 && !isPrepared()) {
			LOG.error(getBeanName() + ":execute: ERROR, params were provided "
					+ "for this static or non-prepared statement "
					+ "that does not require params {} ", getStatement());
			return null;
		}

		// make sure given params match
		if (!params.isEmpty()) {
			if (!isMatch(params.keySet())) {
				LOG.error(getBeanName()
						+ ":execute: ERROR, given key:value set does not "
						+ "match this statement's key:value set\n"
						+ getKeyTokens().toString() + "  vs.  "
						+ params.toString());
				return null;
			}
			LOG.trace(getBeanName() + ":execute: valid param set = "
					+ params.toString());
		}

		Statement stmnt = null;
		ResultSet resultSet = null;
		// execute the statement
		try {
			// get either a bound or simple statement
			stmnt = (isPrepared()) ? cqlStmntPool.getBoundStatement()
					: cqlStmntPool.getSimpleStatement();
			if (stmnt == null) {
				LOG.error(getBeanName()
						+ ":execute: ERROR, getBoundStatement() or "
						+ "getSimpleStatement() returned null");
				return null;
			}

			// now that we've got the statement, set some of its properties
			stmnt.setFetchSize(fetchSize);
			stmnt.setConsistencyLevel(cLevel);
			stmnt.setSerialConsistencyLevel(sLevel);
			stmnt.setIdempotent(idempotent);

			// check for paging. the current paging state (if any) should be in
			// the inMsg
			if (isPagingState() && isSelect()) {
				String pState = (String) inMsg
						.getHeader(CASSANDRA_PAGING_STATE);
				LOG.debug(getBeanName()
						+ ":execute: paging state retrieved = {}", pState);
				if (pState != null) {
					stmnt.setPagingState(PagingState.fromString(pState));
				} else {
					// null removes any state that was previously set on this
					// statement
					stmnt.setPagingState(null);
				}
			} else {
				inMsg.removeHeader(CASSANDRA_PAGING_STATE);
			}

			if (retryPolicy != null) {
				stmnt.setRetryPolicy(retryPolicy);
			}
			if (getDefaultTimestamp() >= 0L) {
				stmnt.setDefaultTimestamp(getDefaultTimestamp());
			}

			if (isPrepared()) {
				LOG.debug("execute: executing this prepared statement {} ",
						getPreparedStr());
				for (String key : params.keySet()) {
					CqlToken token = getKeyTokens().get(key);
					if (token == null) {
						LOG.error(
								getBeanName()
										+ ":execute:this parameter key {}, does not have "
										+ "corresponding parameterized token "
										+ "in this statement {}",
								params.get(key), getStatement());
						return null;
					} else {
						try {
							token.bindObject((BoundStatement) stmnt,
									params.get(key));
						} catch (Exception exc) {
							LOG.error(
									getBeanName()
											+ ":ERROR: while binding objects to bound statement, "
											+ "caught this exception {} for this param {}",
									exc.getClass().getName(), key);
							Utils.dumpStackTrace(exc.getStackTrace());
							return null;
						}
					}
				}
			} else {
				LOG.debug(getBeanName()
						+ ":execute: executing this simple statement {} ",
						getStatement());
			}
			resultSet = session.execute(stmnt);

			// save off the new current paging state
			if (isSelect() && isPagingState()) {
				inMsg.setHeader(CASSANDRA_PAGING_STATE, resultSet
						.getExecutionInfo().getPagingState().toString());
			}

		} catch (Exception exc) {
			LOG.error(getBeanName() + ":execute: caught this exception {}", exc
					.getClass().getName());
			Utils.dumpStackTrace(exc.getStackTrace());
		}
		cqlStmntPool.returnStatement(stmnt);
		return resultSet;
	}

	/**
	 * Returns a String representation of this statement.
	 */
	public String toString() {
		return (isPrepared()) ? getPreparedStr() : getStatement();
	}

	/**
	 * Used for validating CQL statement for select
	 * 
	 * @param cql
	 * @return
	 * @throws IllegalArgumentException
	 */
	private static String valCql4Select(String cql)
			throws IllegalArgumentException {
		if (cql != null && cql.length() > 0) {
			String[] tokens = cql.split(DELIM);
			if (tokens.length < 2) {
				throw new IllegalArgumentException(
						"valSql4Insert: invalid CQL statement - insufficent "
								+ "number of tokens");
			} else if (!tokens[0].equalsIgnoreCase(SELECT_STR)) {
				throw new IllegalArgumentException(
						"valCql4Select: invalid CQL statement - does not start with 'select'");
			}
		} else {
			throw new IllegalArgumentException(
					"valCql4Select: invalid CQL statement - empty or null statement");
		}
		return cql.trim();
	}

	/**
	 * Used for validating CQL statement for insert
	 * 
	 * @param cql
	 * @return
	 * @throws IllegalArgumentException
	 */
	private static String valCql4Insert(String cql)
			throws IllegalArgumentException {
		if (cql != null && cql.length() > 0) {
			String[] tokens = cql.split(DELIM);
			if (tokens.length < 2) {
				throw new IllegalArgumentException(
						"valSql4Insert: invalid CQL statement - insufficent "
								+ "number of tokens");
			} else if (!tokens[0].equalsIgnoreCase(INSERT_STR)) {
				throw new IllegalArgumentException(
						"valCql4Insert: invalid CQL statement - does not start with 'insert'");
			}
		} else {
			throw new IllegalArgumentException(
					"valCql4Insert: invalid CQL statement - empty or null statement");
		}
		return cql.trim();
	}

	/**
	 * Used for validating CQL statement for update
	 * 
	 * @param cql
	 * @return
	 * @throws IllegalArgumentException
	 */
	private static String valCql4Update(String cql)
			throws IllegalArgumentException {
		if (cql != null && cql.length() > 0) {
			String[] tokens = cql.split(DELIM);
			if (tokens.length < 2) {
				throw new IllegalArgumentException(
						"valCql4Update: invalid CQL statement - insufficent "
								+ "number of tokens");
			} else if (!tokens[0].equalsIgnoreCase(UPDATE_STR)) {
				throw new IllegalArgumentException(
						"valCql4Update: invalid CQL statement - must start with "
								+ "'update'");
			}
		} else {
			throw new IllegalArgumentException(
					"valCql4Update: invalid CQL statement - empty or null "
							+ "statement");
		}
		return cql.trim();
	}

	/**
	 * Used for validating CQL statement for DELETE
	 * 
	 * @param cql
	 * @return
	 * @throws IllegalArgumentException
	 */
	private static String valCql4Delete(String cql)
			throws IllegalArgumentException {
		if (cql != null && cql.length() > 0) {
			String[] tokens = cql.split(DELIM);
			if (tokens.length < 2) {
				throw new IllegalArgumentException(
						"valSql4Delete: invalid CQL statement - insufficent "
								+ "number of tokens");
			} else if (!tokens[0].equalsIgnoreCase(DELETE_STR)) {
				throw new IllegalArgumentException(
						"valSql4Delete: invalid CQL statement - must start "
								+ "with 'delete'");
			}
		} else {
			throw new IllegalArgumentException(
					"valSql4Delete: invalid CQL statement - empty or null "
							+ "statement");
		}
		return cql.trim();
	}

	/**
	 * Remove spaces from preparedStr fields. Also ensure preparedStr field is
	 * surrounded by spaces. Care is taken not to modify anything within single
	 * quotes.
	 * 
	 * @param cql
	 * @return
	 */
	private static String removeSpaces(String cql) {
		char[] cqlChar = cql.toCharArray();
		char[] cqlChar2 = new char[cqlChar.length * 2];
		char cstate = SPACE_CHR;
		char pstate = SPACE_CHR;
		int index = 0;
		for (char c : cqlChar) {
			if (cstate == BACK_QUOTE_CHR) {
				// if we're processing a param field, then
				// skip white space chars
				if (c != SPACE_CHR && c != TAB_CHR && c != CARRIAGE_RETURN_CHR
						&& c != NEWLINE_CHR) {
					cqlChar2[index++] = c;
				}
				// if next char just read in is a back tic, then we're
				// finishing up with a param field
				if (c == BACK_QUOTE_CHR) {
					cqlChar2[index++] = SPACE_CHR;
					// exit this state
					cstate = SPACE_CHR;
				}
			} else if (cstate == SINGLE_QUOTE_CHR) {
				// we don't do anything to whatever is within single quotes!
				cqlChar2[index++] = c;
				// leave this state if its an ending single quote char
				// add a space char after the ending single quote
				if (c == SINGLE_QUOTE_CHR) {
					cqlChar2[index++] = SPACE_CHR;
					cstate = SPACE_CHR;
				}
			} else if (cstate != SPACE_CHR || c != SPACE_CHR) {

				// } else {
				pstate = cstate;
				cstate = c;
				// if we've just read in a back tic or comma, then
				// precede it with a space
				if ((cstate == BACK_QUOTE_CHR || cstate == COMMA_CHR)
						&& pstate != SPACE_CHR) {
					cqlChar2[index++] = SPACE_CHR;
				}
				cqlChar2[index++] = cstate;
				// surround commas with spaces
				if (cstate == COMMA_CHR) {
					cqlChar2[index++] = SPACE_CHR;
					cstate = SPACE_CHR;
				}
			}
		}
		return new String(cqlChar2).trim();
	}

	/**
	 * Called to prep and/or initialize this CqlStmnt with the given CqlTokens.
	 * It will also perform some validation.
	 * 
	 * @param orig
	 * @param intokens
	 * @throws IllegalArgumentException
	 * @throws Exception
	 */
	private void init(ArrayList<CqlToken> intokens)
			throws IllegalArgumentException, Exception {

		CqlToken[] keyTokenArray = new CqlToken[intokens.size()];
		int keyTokensIndex = 0;
		String tmpStr = EMPTY_STR;

		// start by iterating through all the tokens, placing them in
		// different buckets based on their type. There are 2 types
		// parameterized and non-parameterized. A non-key is a CQL keyword,
		// while a key is a token for a prepared statement; e.g., `integer:id`
		for (CqlToken token : intokens) {
			if (token.isKey()) {
				keyTokenArray[keyTokensIndex++] = token;
				// and construct the prepared statement
				tmpStr += QUESTION_STR + SPACE_STR;
			} else {
				tmpStr += token.getValue() + SPACE_STR;
			}
			// save 'all' tokens, regardless of type, in the main
			// tokens bucket
			getTokens().add(token);
		}

		// mark the statement as being prepared if it has parameterized
		// tokens
		if (keyTokensIndex > 0) {
			setPreparedStr(tmpStr.trim());
		} else {
			setPreparedStr(EMPTY_STR);
		}

		// look for duplicate parameterized tokens
		for (int i = 0; i < keyTokensIndex; i++) {
			CqlToken token1 = keyTokenArray[i];
			// skip those parameterized tokens that were found to
			// be redundant
			if (token1 == null) {
				continue;
			}
			// now look for duplicate key fields. for example,
			// select `char:field` from student order by `char:field` asc
			// note in the above how the key 'field' is used twice in the
			// select
			for (int j = i + 1; j < keyTokensIndex; j++) {
				CqlToken token2 = keyTokenArray[j];
				// skip those tokens that were found to be redundant and
				// removed
				if (token2 == null) {
					continue;
				}
				// if two tokens have the same key, then
				// they should also have the same types!
				if (token1.getKey().equals(token2.getKey())) {
					if (token1.getCqlType() != token2.getCqlType()) {
						LOG.error("this prepared has duplicate "
								+ "key names, but with different " + "types: "
								+ getStatement());
						throw new IllegalArgumentException(
								"CqlStmnt: duplicate key names in a "
										+ "prepared must have "
										+ "same cql types");
					} else if (!(token1.isCollection() ^ token2.isCollection())) {
						LOG.error("this prepared has duplicate "
								+ "key names, but one is a collection "
								+ "and the other is not: " + getStatement());
						throw new IllegalArgumentException(
								"CqlStmnt: duplicate key names in a "
										+ "prepared must have "
										+ "same collection types");
					} else if (token1.isCollection()
							&& token1.getCollectionType() != token2
									.getCollectionType()) {
						LOG.error("this prepared has duplicate "
								+ "key names, but with different collection "
								+ "types: " + getStatement());
						throw new IllegalArgumentException(
								"CqlStmnt: duplicate key names in a "
										+ "prepared must have "
										+ "same collection types");
					}
					// record the fact that token1 is found
					// in more than one position in this cql statement
					token1.addPosition(token2.getPositions().get(0));
					// token2 is now redundant
					keyTokenArray[j] = null;
				}
			}
		}

		// place the resulting parameterized tokens into the main
		// parameterized tokens map
		for (CqlToken token : keyTokenArray) {
			if (token != null) {
				getKeyTokens().put(token.getKey(), token);
			}
		}

		// based on first token, mark the statement's type
		CqlToken tmpToken = getTokens().get(0);
		if (tmpToken.getValue().equalsIgnoreCase(SELECT_STR)) {
			setCqlStmntType(Method.SELECT);
			// is this a SELECT JSON ... statement?
			setJsonSelect(getTokens().get(1).isJson());
		} else if (tmpToken.getValue().equalsIgnoreCase(DELETE_STR)) {
			setCqlStmntType(Method.DELETE);

		} else if (tmpToken.getValue().equalsIgnoreCase(UPDATE_STR)) {
			setCqlStmntType(Method.UPDATE);

		} else if (tmpToken.getValue().equalsIgnoreCase(INSERT_STR)) {
			setCqlStmntType(Method.INSERT);

		} else {
			throw new IllegalArgumentException("unknown CQL statement: "
					+ tmpToken.getValue());
		}
	}

	/**
	 * @return the statement
	 */
	public String getStatement() {
		return statement;
	}

	/**
	 * @param statement
	 *            the statement to set
	 */
	public void setStatement(String statement) {
		this.statement = statement;
	}

	/**
	 * @return the fetchSize
	 */
	public int getFetchSize() {
		return fetchSize;
	}

	/**
	 * @param fetchSize
	 *            the fetchSize to set
	 */
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	/**
	 * @return the pagingState
	 */
	public boolean isPagingState() {
		return pagingState;
	}

	/**
	 * @param pagingState
	 *            the pagingState to set
	 */
	public void setPagingState(boolean pagingState) {
		this.pagingState = pagingState;
	}

	/**
	 * @return the tracing
	 */
	public boolean isTracing() {
		return tracing;
	}

	/**
	 * @param tracing
	 *            the tracing to set
	 */
	public void setTracing(boolean tracing) {
		this.tracing = tracing;
	}

	/**
	 * @return the defaultTimestamp
	 */
	public long getDefaultTimestamp() {
		return defaultTimestamp;
	}

	/**
	 * @param defaultTimestamp
	 *            the defaultTimestamp to set
	 */
	public void setDefaultTimestamp(long defaultTimestamp) {
		this.defaultTimestamp = defaultTimestamp;
	}

	/**
	 * @return the idempotent
	 */
	public boolean isIdempotent() {
		return idempotent;
	}

	/**
	 * @param idempotent
	 *            the idempotent to set
	 */
	public void setIdempotent(boolean idempotent) {
		this.idempotent = idempotent;
		this.myIdempotent = new Boolean(idempotent);
	}

	/**
	 * @return the retryPolicy
	 */
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	/**
	 * @param retryPolicy
	 *            the retryPolicy to set
	 */
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * @return the consistencyLevel
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	/**
	 * @param consistencyLevel
	 *            the consistencyLevel to set
	 */
	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * @return the serialConsistencyLevel
	 */
	public ConsistencyLevel getSerialConsistencyLevel() {
		return serialConsistencyLevel;
	}

	/**
	 * @param serialConsistencyLevel
	 *            the serialConsistencyLevel to set
	 */
	public void setSerialConsistencyLevel(
			ConsistencyLevel serialConsistencyLevel) {
		this.serialConsistencyLevel = serialConsistencyLevel;
	}

	@Override
	public void setBeanName(String name) {
		beanName = name;
	}

	public String getBeanName() {
		return beanName;
	}

	/**
	 * @return the myIdempotent
	 */
	public Boolean getMyIdempotent() {
		return myIdempotent;
	}

	/**
	 * @return the stmntPool
	 */
	public Map<Session, CqlStmntPool> getStmntPool() {
		return stmntPool;
	}

	/**
	 * @param stmntPool
	 *            the stmntPool to set
	 */
	public void setStmntPool(Map<Session, CqlStmntPool> stmntPool) {
		this.stmntPool = stmntPool;
	}

	/**
	 * @return the isJsonSelect
	 */
	public boolean isJsonSelect() {
		return isJsonSelect;
	}

	/**
	 * @param isJsonSelect
	 *            the isJsonSelect to set
	 */
	public void setJsonSelect(boolean isJsonSelect) {
		this.isJsonSelect = isJsonSelect;
	}

	/**
	 * @return the stackSize
	 */
	public int getStackSize() {
		return stackSize;
	}

	/**
	 * @param stackSize
	 *            the stackSize to set
	 */
	public void setStackSize(int stackSize) {
		this.stackSize = stackSize;
	}

	/*
	 * Every CQLStmnt has a pool of simple and bound statements. Note that a
	 * CQLStmnt is a thread safe singleton bean that is concurrently accessed by
	 * many threads of execution. You can have multiple client beans injected
	 * with the same CqlSmnt singleton bean and each of those Clients will use
	 * their own distinct Cassandra session. Therefore, each CqlStmnt bean
	 * maintains a Map of these pools; one pool for each session. We don't want
	 * to be constantly creating and destroying these Simple and Bound
	 * statements. Instead, we'd like to reuse them and thus the pool.
	 */
	private class CqlStmntPool {

		private Stack<SimpleStatement> simpleStack = new Stack<SimpleStatement>();
		private Stack<BoundStatement> boundStack = new Stack<BoundStatement>();
		// used only if this CQL statement is a prepared statement
		private PreparedStatement preparedStatement;

		CqlStmntPool() {
		}

		/**
		 * Note that a PreparedStatement object allows you to define specific
		 * defaults for the different properties of a Statement (Consistency
		 * level, tracing, ...), in which case those properties will be
		 * inherited, as default, by every BoundStatement created from the
		 * PreparedStatement.
		 * 
		 * TODO: can we reuse the BoundStatement and SimpleStatement?
		 * 
		 * @return a boundStatement from this prepared statement
		 */
		BoundStatement getBoundStatement() {
			if (!isPrepared()) {
				LOG.warn("Attempt to get bound statement from non-prepared statement");
				return null;
			}
			synchronized (boundStack) {
				if (!boundStack.isEmpty()) {
					return boundStack.pop();
				}
			}
			return getPreparedStatement().bind();
		}

		SimpleStatement getSimpleStatement() {
			if (isPrepared()) {
				LOG.warn("Attempt to get simple statement from prepared statement");
				return null;
			}
			synchronized (simpleStack) {
				if (!simpleStack.isEmpty()) {
					return simpleStack.pop();
				}
			}
			return new SimpleStatement(getStatement());
		}

		void returnStatement(Statement stmnt) {
			if (stmnt != null) {
				if (stmnt instanceof BoundStatement) {
					synchronized (boundStack) {
						if (boundStack.size() < getStackSize()) {
							boundStack.push((BoundStatement) stmnt);
						}
					}
				} else {
					synchronized (simpleStack) {
						if (simpleStack.size() < getStackSize()) {
							simpleStack.push((SimpleStatement) stmnt);
						}
					}
				}
			}
		}

		/**
		 * @return the preparedStatement
		 */
		public PreparedStatement getPreparedStatement() {
			return preparedStatement;
		}

		/**
		 * @param preparedStatement
		 *            the preparedStatement to set
		 */
		public void setPreparedStatement(PreparedStatement preparedStatement) {
			this.preparedStatement = preparedStatement;
		}

	}

}
