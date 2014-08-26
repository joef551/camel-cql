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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

import org.apache.camel.Message;
import org.metis.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.metis.utils.Constants.*;

/**
 * Object that encapsulates or represents a CQL statement. This object is
 * created by a Client during its lifecycle's initialization phase.
 */
public class CqlStmnt {

	public static final Logger LOG = LoggerFactory.getLogger(CqlStmnt.class);

	// the cassandra session used by this statement
	private Session session;

	// this is the string representation of this CQL statement as provided by
	// the Spring application context.
	private String originalStr;

	// this is the string version of the prepared statement for this CQL
	// statement; i.e., assuming this is a prepared statement.
	private String preparedStr = "";

	// used only if this CQL statement is a prepared statement
	private PreparedStatement preparedStatement;

	// this list contains 'all' the tokens that comprise this CQL statement;
	// in other words, it contains both regular and key/parameterized tokens
	private ArrayList<CqlToken> tokens = new ArrayList<CqlToken>();

	// this map contains only the parameterized key tokens; e.g., `int:id` is a
	// parameterized token.
	private Map<String, CqlToken> keyTokens = new HashMap<String, CqlToken>();

	private int dfltFetchSize;
	private ConsistencyLevel dftlConsistencyLevel;
	private ConsistencyLevel dftlSerialConsistencyLevel;

	// Enumeration used for identifying the type of CQL statement
	public enum CqlStmntType {
		SELECT, UPDATE, DELETE, INSERT;
	}

	// the type of CQL statement that this statement represents; e.g., select,
	// insert, update, etc.
	private CqlStmntType cqlStmntType;

	private static final int MAX_STACK_SIZE = 50;
	private Stack<SimpleStatement> simpleStack = new Stack<SimpleStatement>();
	private Stack<BoundStatement> boundStack = new Stack<BoundStatement>();

	/**
	 * Create a CqlStmnt object from the given CQL string and input tokens. The
	 * tokens were previously created by the static getCQLStmnt method.
	 * 
	 * @param originalStr
	 * @param intokens
	 * @throws IllegalArgumentException
	 */
	public CqlStmnt(String orig, ArrayList<CqlToken> intokens, Session session)
			throws IllegalArgumentException, Exception {
		setSession(session);
		setOriginalStr(orig);
		setDfltFetchSize(session.getCluster().getConfiguration()
				.getQueryOptions().getFetchSize());
		setDftlConsistencyLevel(session.getCluster().getConfiguration()
				.getQueryOptions().getConsistencyLevel());
		setDftlSerialConsistencyLevel(session.getCluster().getConfiguration()
				.getQueryOptions().getSerialConsistencyLevel());
		// perform all the initialization and validation tasks for this new
		// CqlStmnt
		init(intokens);
	}

	/**
	 * Get the CqlStmntType for this statement.
	 * 
	 * @return
	 */
	public CqlStmntType getCqlStmntType() {
		return cqlStmntType;
	}

	/**
	 * Set the CqlStmntType for this statement
	 * 
	 * @param cqlStmntType
	 */
	public void setCqlStmntType(CqlStmntType cqlStmntType) {
		this.cqlStmntType = cqlStmntType;
	}

	/**
	 * Get the String representation as originally injected.
	 * 
	 * @return
	 */
	public String getOriginalStr() {
		return originalStr;
	}

	public void setOriginalStr(String originalStr) {
		this.originalStr = originalStr;
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
		return this.getCqlStmntType() == CqlStmntType.SELECT;
	}

	public boolean isDelete() {
		return this.getCqlStmntType() == CqlStmntType.DELETE;
	}

	public boolean isInsert() {
		return this.getCqlStmntType() == CqlStmntType.INSERT;
	}

	public boolean isUpdate() {
		return this.getCqlStmntType() == CqlStmntType.UPDATE;
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
			LOG.trace("getMatch: null or empty list of statements "
					+ "was provided");
			return null;
		} else {
			LOG.trace("getMatch: {} statements are in provided list ",
					stmnts.size());

		}

		// if there are no keys, then follow step 1
		if (keys == null || keys.isEmpty()) {
			LOG.trace("getMatch: input params were not provided");
			// first search for the first non-prepared statement
			for (CqlStmnt stmnt : stmnts) {
				if (!stmnt.isPrepared()) {
					LOG.trace("getMatch: returning this non-prepared statement: "
							+ stmnt.getOriginalStr());
					return stmnt;
				}
			}
			LOG.trace("getMatch: returning null");
			return null;
		}

		// We have keys to work with so we'll work off the given keys
		LOG.trace("getMatch: these input params were provided: "
				+ keys.toString());
		for (CqlStmnt stmnt : stmnts) {
			LOG.trace("getMatch: checking against this statement {}",
					stmnt.getOriginalStr());
			if (stmnt.isMatch(keys)) {
				LOG.trace("getMatch: returning this statement: "
						+ stmnt.getOriginalStr());
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
	public static CqlStmnt getCQLStmnt(String cql, Session session)
			throws IllegalArgumentException, Exception {

		if (cql == null || cql.isEmpty()) {
			throw new IllegalArgumentException(
					"getCQLTokens: cql string is null or empty");
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
		String cql2 = removeSpaces(cql);

		// split the cql string into tokens where the delimiter is any number of
		// white spaces. a cql statement must have at least 2 tokens
		String[] tokens = cql2.trim().split(DELIM);
		if (tokens == null || tokens.length < 2) {
			throw new IllegalArgumentException(
					"Invalid CQL statement - insufficent number of tokens");
		}

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
		// create and return a CqlStmnt from the given tokens
		return new CqlStmnt(cql, tList, session);
	}

	/**
	 * Called by the Client to execute this CQL statement with the given params.
	 * 
	 * @param params
	 * @throws CQLException
	 */
	public ResultSet execute(Map<String, Object> inParams, Message inMsg) {

		Map<String, Object> params = (inParams == null) ? new HashMap<String, Object>()
				: inParams;

		LOG.debug("execute: executing this original statement: {} ",
				getOriginalStr());
		LOG.debug("execute: executing this prepared statement: {} ",
				getPreparedStr());
		LOG.debug("execute: executing with this number {} of params",
				params.size());

		// look for session level params in the in message
		ConsistencyLevel cLevel = getDftlConsistencyLevel();
		ConsistencyLevel sLevel = getDftlSerialConsistencyLevel();
		int fetchSize = getDfltFetchSize();
		Object tmpObj = inMsg.getHeader(CASSANDRA_CONSISTENCY_LEVEL);
		if (tmpObj != null) {
			try {
				cLevel = ConsistencyLevel.valueOf((String) tmpObj);
			} catch (Exception e) {
				LOG.warn(
						"execute: Invalid consistency level {} in map, using default",
						(String) tmpObj);
			}
		}
		tmpObj = inMsg.getHeader(CASSANDRA_SERIAL_CONSISTENCY_LEVEL);
		if (tmpObj != null) {
			try {
				sLevel = ConsistencyLevel.valueOf((String) tmpObj);
			} catch (Exception e) {
				LOG.warn(
						"execute: Invalid serial consistency level {} in map, using default",
						(String) tmpObj);
			}
		}
		tmpObj = inMsg.getHeader(CASSANDRA_FETCH_SIZE);
		if (tmpObj != null) {
			try {
				fetchSize = Integer.valueOf((String) tmpObj);
			} catch (Exception e) {
				LOG.warn(
						"execute: Invalid fetch size {} in map, using default",
						(String) tmpObj);
			}
		}

		// first, do some light validation work
		if (params.size() == 0 && isPrepared()) {
			LOG.error("execute: ERROR, params were not provided "
					+ "for this prepared statement {}", getPreparedStr());
			return null;
		} else if (params.size() > 0 && !isPrepared()) {
			LOG.error(
					"execute: ERROR, params were provided "
							+ "for this static or non-prepared statement that does not "
							+ "require params {} ", getOriginalStr());
			return null;
		}

		// make sure given params match
		if (params.size() > 0) {
			if (!isMatch(params.keySet())) {
				LOG.error("execute: ERROR, given key:value set does not "
						+ "match this statement's key:value set\n"
						+ getKeyTokens().toString() + "  vs.  "
						+ params.toString());
				return null;
			}
			LOG.trace("execute: valid param set = " + params.toString());
		}

		Statement stmnt = null;
		// execute the statement
		try {

			// get either a bound or simple statement
			stmnt = (isPrepared()) ? getBoundStatement() : getSimpleStatement();
			stmnt.setFetchSize(fetchSize);
			stmnt.setConsistencyLevel(cLevel);
			stmnt.setSerialConsistencyLevel(sLevel);

			if (LOG.isTraceEnabled()) {
				LOG.trace("execute: fetch size = " + stmnt.getFetchSize());
				LOG.trace("execute: consistency level = "
						+ stmnt.getConsistencyLevel());
				LOG.trace("execute: serial consistency level = "
						+ stmnt.getSerialConsistencyLevel());
			}

			if (isPrepared()) {
				LOG.debug("execute: executing this prepared statement {} ",
						getPreparedStr());
				for (String key : params.keySet()) {
					CqlToken token = getKeyTokens().get(key);
					if (token == null) {
						LOG.error("this parameter key {}, does not have "
								+ "corresponding parameterized token "
								+ "in this statement {}", params.get(key),
								getOriginalStr());
						return null;
					} else {
						try {
							token.bindObject((BoundStatement) stmnt,
									params.get(key));
						} catch (Exception exc) {
							LOG.error(
									"ERROR: while binding objects to bound statement, "
											+ "caught this exception {} for this param {}",
									exc.getClass().getName(), key);
							Utils.dumpStackTrace(exc.getStackTrace());
							return null;
						}
					}
				}
				return session.execute((SimpleStatement) stmnt);
			}
			LOG.debug("execute: executing this simple statement {} ",
					getOriginalStr());
			SimpleStatement sStmnt = getSimpleStatement();
			sStmnt.setFetchSize(fetchSize);
			sStmnt.setConsistencyLevel(cLevel);
			sStmnt.setSerialConsistencyLevel(sLevel);
			return session.execute(getOriginalStr());

		} catch (Exception exc) {
			LOG.error("execute: caught this exception {}", exc.getClass()
					.getName());
			Utils.dumpStackTrace(exc.getStackTrace());
		} finally {
			returnStatement(stmnt);
		}
		return null;
	}

	/**
	 * Returns a String representation of this statement.
	 */
	public String toString() {
		return (isPrepared()) ? getPreparedStr() : getOriginalStr();
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
		int index = 0;
		for (char c : cqlChar) {
			if (cstate == BACK_QUOTE_CHR) {
				if (c != SPACE_CHR && c != TAB_CHR && c != CARRIAGE_RETURN_CHR
						&& c != NEWLINE_CHR) {
					cqlChar2[index++] = c;
				}
				if (c == BACK_QUOTE_CHR) {
					cqlChar2[index++] = SPACE_CHR;
					cstate = SPACE_CHR;
				}
			} else if (cstate == SINGLE_QUOTE_CHR) {
				cqlChar2[index++] = c;
				if (c == SINGLE_QUOTE_CHR) {
					cstate = SPACE_CHR;
				}
			} else {
				cstate = c;
				if (cstate == BACK_QUOTE_CHR || cstate == COMMA_CHR) {
					cqlChar2[index++] = SPACE_CHR;
				}
				cqlChar2[index++] = cstate;
				// surround commas with spaces
				if (cstate == COMMA_CHR) {
					cqlChar2[index++] = SPACE_CHR;
				}
			}
		}
		return new String(cqlChar2).trim();
	}

	/**
	 * Called by constructor to prep or initialize this CqlStmnt with the given
	 * CqlTokens. It will also perform some validation.
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
			setPreparedStatement(session.prepare(preparedStr));
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
								+ getOriginalStr());
						throw new IllegalArgumentException(
								"CqlStmnt: duplicate key names in a "
										+ "prepared must have "
										+ "same cql types");
					} else if (!(token1.isCollection() ^ token2.isCollection())) {
						LOG.error("this prepared has duplicate "
								+ "key names, but one is a collection "
								+ "and the other is not: " + getOriginalStr());
						throw new IllegalArgumentException(
								"CqlStmnt: duplicate key names in a "
										+ "prepared must have "
										+ "same collection types");
					} else if (token1.isCollection()
							&& token1.getCollectionType() != token2
									.getCollectionType()) {
						LOG.error("this prepared has duplicate "
								+ "key names, but with different collection "
								+ "types: " + getOriginalStr());
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
			setCqlStmntType(CqlStmntType.SELECT);

		} else if (tmpToken.getValue().equalsIgnoreCase(DELETE_STR)) {
			setCqlStmntType(CqlStmntType.DELETE);

		} else if (tmpToken.getValue().equalsIgnoreCase(UPDATE_STR)) {
			setCqlStmntType(CqlStmntType.UPDATE);

		} else if (tmpToken.getValue().equalsIgnoreCase(INSERT_STR)) {
			setCqlStmntType(CqlStmntType.INSERT);

		} else {
			throw new IllegalArgumentException("unknown CQL statement: "
					+ tmpToken.getValue());
		}
	}

	/**
	 * @return the session
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * @param session
	 *            the session to set
	 */
	public void setSession(Session session) {
		this.session = session;
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

	/**
	 * Note that a PreparedStatement object allows you to define specific
	 * defaults for the different properties of a Statement (Consistency level,
	 * tracing, ...), in which case those properties will be inherited, as
	 * default, by every BoundStatement created from the PreparedStatement.
	 * 
	 * TODO: can we reuse the BoundStatement and SimpleStatement?
	 * 
	 * @return a boundStatement from this prepared statement
	 */
	public BoundStatement getBoundStatement() {
		if (!isPrepared()) {
			return null;
		}
		synchronized (boundStack) {
			if (!boundStack.isEmpty()) {
				return boundStack.pop();
			}
		}
		return getPreparedStatement().bind();
	}

	public SimpleStatement getSimpleStatement() {
		if (isPrepared()) {
			return null;
		}
		synchronized (simpleStack) {
			if (!simpleStack.isEmpty()) {
				return simpleStack.pop();
			}
		}
		return new SimpleStatement(getOriginalStr());
	}

	public void returnStatement(Statement stmnt) {
		if (stmnt != null) {
			if (stmnt instanceof BoundStatement) {
				synchronized (boundStack) {
					if (boundStack.size() < MAX_STACK_SIZE) {
						boundStack.push((BoundStatement) stmnt);
					}
				}
			} else {
				synchronized (simpleStack) {
					if (simpleStack.size() < MAX_STACK_SIZE) {
						simpleStack.push((SimpleStatement) stmnt);
					}
				}
			}
		}
	}

	/**
	 * @return the dfltFetchSize
	 */
	public int getDfltFetchSize() {
		return dfltFetchSize;
	}

	/**
	 * @param dfltFetchSize
	 *            the dfltFetchSize to set
	 */
	public void setDfltFetchSize(int dfltFetchSize) {
		this.dfltFetchSize = dfltFetchSize;
	}

	/**
	 * @return the dftlConsistencyLevel
	 */
	public ConsistencyLevel getDftlConsistencyLevel() {
		return dftlConsistencyLevel;
	}

	/**
	 * @param dftlConsistencyLevel
	 *            the dftlConsistencyLevel to set
	 */
	public void setDftlConsistencyLevel(ConsistencyLevel dftlConsistencyLevel) {
		this.dftlConsistencyLevel = dftlConsistencyLevel;
	}

	/**
	 * @return the dftlSerialConsistencyLevel
	 */
	public ConsistencyLevel getDftlSerialConsistencyLevel() {
		return dftlSerialConsistencyLevel;
	}

	/**
	 * @param dftlSerialConsistencyLevel
	 *            the dftlSerialConsistencyLevel to set
	 */
	public void setDftlSerialConsistencyLevel(
			ConsistencyLevel dftlSerialConsistencyLevel) {
		this.dftlSerialConsistencyLevel = dftlSerialConsistencyLevel;
	}

}
