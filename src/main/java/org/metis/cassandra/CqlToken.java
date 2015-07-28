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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;

import org.metis.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.metis.utils.Constants.JSON_STR;

/**
 * Represents a string token of a CQL statement. There are two types: regular
 * and key. A regular token is a CQL key word, while a key-token represents a
 * parameterized field of the CQL statement. A parameterized field is comprised
 * of two or three parts (cql-type, collection-type, key) and is delimited by
 * '`'. For example, `ascii:first` represents a String whose key name is
 * 'first'. For example: <code>
 * select first from users where first like `string:first` || '%'
 * </code> Another example, `set:long:user_ids` represents a set of Longs where
 * element in the set is a user id.
 * 
 */
public class CqlToken {

	public static final Logger LOG = LoggerFactory.getLogger(CqlToken.class);

	private String key;
	private String value;
	private DataType.Name cqlType;
	private DataType.Name collectionType;
	// this token may be replicated across a single statement
	private List<Integer> positions = new ArrayList<Integer>();

	/**
	 * Create a parameterized token
	 * 
	 * @param sqlType
	 * @param key
	 * @param mode
	 * @param position
	 * @throws IllegalArgumentException
	 */
	public CqlToken(String cqlType, String key, int position)
			throws IllegalArgumentException {

		// convert the given cqlType to an enumerated value
		this.cqlType = DataType.Name.valueOf(cqlType.toUpperCase());

		// a tuple is a special case of a collection in that arbitrary types can
		// be added to a tuple
		if (this.cqlType == DataType.Name.TUPLE) {
			this.collectionType = this.cqlType;
		}

		// the key, which always equals the value propert, is used to id this
		// token as a parameter field as opposed to a token that represents a
		// CQL keyword. A keyword token will have its key property set to null
		this.key = key.toLowerCase();
		this.value = this.key;
		addPosition(position);
	}

	/**
	 * Create a parameterized token for a collection type. This is only called
	 * when creating a collection type like `list:text:email`, which is
	 * <collectionType>:<cqlType>:<key>
	 * 
	 * @param cqlType
	 *            : SET, LIST, or MAP
	 * @param collectionType
	 *            : long, string, int, ... etc
	 * @param key
	 * @param position
	 * @throws IllegalArgumentException
	 */
	public CqlToken(String cqlType, String collectionType, String key,
			int position) throws IllegalArgumentException {

		this(cqlType, key, position);
		switch (getCqlType()) {
		case SET:
		case LIST:
		case MAP:
			break;
		default:
			throw new IllegalArgumentException("invalid CqlType for collection");
		}
		// convert the given collection type to a CqlType.
		this.collectionType = DataType.Name.valueOf(collectionType
				.toUpperCase());
		switch (getCollectionType()) {
		case SET:
		case LIST:
		case MAP:
			// You can't define something like this: `list:list:email` or
			// `list:map:email`
			throw new IllegalArgumentException(
					"collection type for a collection cannot be another collection");
		default:
			break;
		}
	}

	public static boolean isCollection(DataType.Name type) {
		return type.isCollection();
	}

	/**
	 * Create a non-parameterized token.
	 * 
	 * @param value
	 */
	public CqlToken(String value) {
		this.value = value;
	}

	public void addPosition(Integer pos) {
		getPositions().add(pos);
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public boolean isKey() {
		return key != null;
	}

	public boolean isJson() {
		// so in order for this to return true, isKey must return false and the
		// value of this token must equal "json"
		return !isKey() && getValue().equalsIgnoreCase(JSON_STR);
	}

	public DataType.Name getCqlType() {
		return cqlType;
	}

	public List<Integer> getPositions() {
		return positions;
	}

	public boolean isCollection() {
		return getCollectionType() != null;
	}

	/**
	 * Binds an object to a BoundStatement
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void bindObject(BoundStatement bs, Object obj) throws Exception {
		LOG.trace("bindObject: entered with this type {}", obj.getClass()
				.getName());
		if (obj instanceof String) {
			bindString(bs, (String) obj);
		} else if (obj instanceof Map) {
			bindMap(bs, (Map) obj);
		} else if (obj instanceof List) {
			if (getCollectionType() == DataType.Name.TUPLE) {
				bindTuple(bs, (List) obj);
			} else {
				bindList(bs, (List) obj);
			}
		} else if (obj instanceof Set) {
			bindSet(bs, (Set) obj);
		} else {
			throw new Exception("unknown type of " + obj.getClass().getName());
		}
	}

	/**
	 * Returns a Java object for this parameterized token. The object's value is
	 * taken from the given String. This method excludes collection types.
	 * 
	 * @param value
	 * @return
	 * @throws UnknownHostException
	 */
	public Object getObjectValue(String value) throws NumberFormatException,
			IllegalArgumentException, MalformedURLException,
			UnknownHostException {

		if (!isKey()) {
			return null;
		} else if (isCollection()) {
			throw new IllegalArgumentException(
					"string value cannot be converted to collection");
		}

		switch (getCqlType()) {
		case BLOB:
			return ByteBuffer.wrap(value.getBytes());
		case DECIMAL:
			return new BigDecimal(value);
		case VARINT:
			return new BigInteger(value);
		case BOOLEAN:
			if (value.trim().equalsIgnoreCase("true")
					|| value.trim().equalsIgnoreCase("false")) {
				return Boolean.valueOf(value);
			} else {
				LOG.error("this value is set to neither 'true' nor 'false' :"
						+ value);
				throw new NumberFormatException(
						"this value is set to neither 'true' nor 'false' :"
								+ value);
			}
		case INET:
			return InetAddress.getByName(value);
		case INT:
			return Integer.valueOf(value);
		case BIGINT:
		case COUNTER:
			return Long.valueOf(value);
		case FLOAT:
			return Float.valueOf(value);
		case DOUBLE:
			return Double.valueOf(value);
		case TIMESTAMP:
			return Timestamp.valueOf(value);
		case TIMEUUID:
		case UUID:
			return UUID.fromString(value);
		default:
			// if it is none of the above, then it is a String type
			return value;
		}
	}

	public static Object getObjectFromRow(Row row, String colName,
			DataType.Name type) {
		switch (type) {
		case BLOB:
			return row.getBytes(colName);
		case DECIMAL:
			return row.getDecimal(colName);
		case VARINT:
			return row.getVarint(colName);
		case BOOLEAN:
			return row.getBool(colName);
		case INET:
			return row.getInet(colName);
		case INT:
			return row.getInt(colName);
		case BIGINT:
		case COUNTER:
			return row.getLong(colName);
		case FLOAT:
			return row.getFloat(colName);
		case DOUBLE:
			return row.getDouble(colName);
		case TIMESTAMP:
			return row.getDate(colName);
		case TIMEUUID:
		case UUID:
			return row.getUUID(colName);
		case SET:
			return row.getSet(colName, String.class);
		case LIST:
			return row.getList(colName, String.class);
		case MAP:
			return row.getMap(colName, String.class, String.class);
		case TUPLE:
			// A tuple would have to be returned as a List of Objects
			List<Object> tupleObjs = new ArrayList<Object>();
			TupleValue tv = row.getTupleValue(colName);
			// get a bead on the number of values in the tuple
			int numValues = tv.getType().getComponentTypes().size();
			// copy corresponding objects to list
			for (int i = 0; i < numValues; i++) {
				tupleObjs.add(tv.getObject(i));
			}
			return tupleObjs;
		default:
			return row.getString(colName);
		}
	}

	/**
	 * Bind the object to the bound statement.
	 * 
	 * @param bs
	 * @param value
	 * @throws Exception
	 */
	public void bindString(BoundStatement bs, String value) throws Exception {
		if (isCollection()) {
			throw new Exception(
					"attempting to bind single object for collection");
		}
		switch (getCqlType()) {
		case BLOB:
			ByteBuffer bb = (ByteBuffer) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						bb.toString(), pos);
				bs.setBytes(pos, bb);
			}
			break;
		case DECIMAL:
			BigDecimal bd = (BigDecimal) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						bd.toString(), pos);
				bs.setDecimal(pos, bd);
			}
			break;
		case VARINT:
			BigInteger bi = (BigInteger) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						bi.toString(), pos);
				bs.setVarint(pos, bi);
			}
			break;
		case BOOLEAN:
			Boolean b = (Boolean) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						b.toString(), pos);
				bs.setBool(pos, b);
			}
			break;
		case INET:
			InetAddress inet = (InetAddress) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						inet.toString(), pos);
				bs.setInet(pos, inet);
			}
			break;
		case INT:
			Integer integer = (Integer) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						integer.toString(), pos);
				bs.setInt(pos, integer);
			}
			break;
		case BIGINT:
		case COUNTER:
			Long ilong = (Long) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						ilong.toString(), pos);
				bs.setLong(pos, ilong);
			}
			break;
		case FLOAT:
			Float iFloat = (Float) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						iFloat.toString(), pos);
				bs.setFloat(pos, iFloat);
			}
			break;
		case DOUBLE:
			Double iDouble = (Double) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						iDouble.toString(), pos);
				bs.setDouble(pos, iDouble);
			}
			break;
		case TIMESTAMP:
			Timestamp iDate = (Timestamp) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						iDate.toString(), pos);
				bs.setDate(pos, iDate);
			}
			break;
		case TIMEUUID:
		case UUID:
			UUID iUUID = (UUID) getObjectValue(value);
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						iUUID.toString(), pos);
				bs.setUUID(pos, iUUID);
			}
			break;
		default:
			// if it is none of the above, then it is a String type
			for (Integer pos : getPositions()) {
				LOG.trace("bindString: binding {} to position {}",
						value.toString(), pos);
				bs.setString(pos, value);
			}
			break;
		}
	}

	/**
	 * Based on given Map, returns appropriate populated collection for this
	 * collection type.
	 * 
	 * @param inMap
	 * @return
	 * @throws Exception
	 */
	public Map<String, ?> getMap(Map<String, String> inMap) throws Exception {
		if (!isCollection()) {
			throw new Exception(
					"attempting to get collection for single object type");
		} else if (getCqlType() != DataType.Name.MAP) {
			throw new Exception("invalid bind: attempting to bind Map for "
					+ getCqlType());
		}
		switch (getCollectionType()) {
		case BLOB: {
			Map<String, ByteBuffer> map = new HashMap<String, ByteBuffer>();
			for (String key : inMap.keySet()) {
				map.put(key, ByteBuffer.wrap(inMap.get(key).getBytes()));
			}
			return map;
		}
		case DECIMAL: {
			Map<String, BigDecimal> map = new HashMap<String, BigDecimal>();
			for (String key : inMap.keySet()) {
				map.put(key, new BigDecimal(inMap.get(key)));
			}
			return map;
		}
		case VARINT: {
			Map<String, BigInteger> map = new HashMap<String, BigInteger>();
			for (String key : inMap.keySet()) {
				map.put(key, new BigInteger(inMap.get(key)));
			}
			return map;
		}
		case BOOLEAN: {
			Map<String, Boolean> map = new HashMap<String, Boolean>();
			for (String key : inMap.keySet()) {
				map.put(key, Boolean.valueOf(inMap.get(key)));
			}
			return map;
		}
		case INET: {
			Map<String, InetAddress> map = new HashMap<String, InetAddress>();
			for (String key : inMap.keySet()) {
				map.put(key, InetAddress.getByName(inMap.get(key)));
			}
			return map;
		}
		case INT: {
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (String key : inMap.keySet()) {
				map.put(key, Integer.valueOf(inMap.get(key)));
			}
			return map;
		}
		case BIGINT:
		case COUNTER: {
			Map<String, Long> map = new HashMap<String, Long>();
			for (String key : inMap.keySet()) {
				map.put(key, Long.valueOf(inMap.get(key)));
			}
			return map;
		}
		case FLOAT: {
			Map<String, Float> map = new HashMap<String, Float>();
			for (String key : inMap.keySet()) {
				map.put(key, Float.valueOf(inMap.get(key)));
			}
			return map;
		}
		case DOUBLE: {
			Map<String, Double> map = new HashMap<String, Double>();
			for (String key : inMap.keySet()) {
				map.put(key, Double.valueOf(inMap.get(key)));
			}
			return map;
		}
		case TIMESTAMP: {
			Map<String, Date> map = new HashMap<String, Date>();
			for (String key : inMap.keySet()) {
				map.put(key, Date.valueOf(inMap.get(key)));
			}
			return map;
		}
		case TIMEUUID:
		case UUID: {
			Map<String, UUID> map = new HashMap<String, UUID>();
			for (String key : inMap.keySet()) {
				map.put(key, UUID.fromString(inMap.get(key)));
			}
			return map;
		}
		default:
			return inMap;
		}
	}

	public Set<?> getSet(Set<String> inSet) throws Exception {
		if (!isCollection()) {
			throw new Exception(
					"attempting to get collection for single object type");
		} else if (getCqlType() != DataType.Name.SET) {
			throw new Exception("invalid bind: attempting to bind Set for "
					+ getCqlType());
		}
		switch (getCollectionType()) {
		case BLOB: {
			Set<ByteBuffer> set = new HashSet<ByteBuffer>();
			for (String val : inSet) {
				set.add(ByteBuffer.wrap(val.getBytes()));
			}
			return set;
		}
		case DECIMAL: {
			Set<BigDecimal> set = new HashSet<BigDecimal>();
			for (String val : inSet) {
				set.add(new BigDecimal(val));
			}
			return set;
		}
		case VARINT: {
			Set<BigInteger> set = new HashSet<BigInteger>();
			for (String val : inSet) {
				set.add(new BigInteger(val));
			}
			return set;
		}
		case BOOLEAN: {
			Set<Boolean> set = new HashSet<Boolean>();
			for (String val : inSet) {
				set.add(Boolean.valueOf(val));
			}
			return set;
		}
		case INET: {
			Set<InetAddress> set = new HashSet<InetAddress>();
			for (String val : inSet) {
				set.add(InetAddress.getByName(val));
			}
			return set;
		}
		case INT: {
			Set<Integer> set = new HashSet<Integer>();
			for (String val : inSet) {
				set.add(Integer.valueOf(val));
			}
			return set;
		}
		case BIGINT:
		case COUNTER: {
			Set<Long> set = new HashSet<Long>();
			for (String val : inSet) {
				set.add(Long.valueOf(val));
			}
			return set;
		}
		case FLOAT: {
			Set<Float> set = new HashSet<Float>();
			for (String val : inSet) {
				set.add(Float.valueOf(val));
			}
			return set;
		}
		case DOUBLE: {
			Set<Double> set = new HashSet<Double>();
			for (String val : inSet) {
				set.add(Double.valueOf(val));
			}
			return set;
		}
		case TIMESTAMP: {
			Set<Date> set = new HashSet<Date>();
			for (String val : inSet) {
				set.add(Date.valueOf(val));
			}
			return set;
		}
		case TIMEUUID:
		case UUID: {
			Set<UUID> set = new HashSet<UUID>();
			for (String val : inSet) {
				set.add(UUID.fromString(val));
			}
			return set;
		}
		default:
			return inSet;
		}
	}

	public List<?> getList(List<String> inList) throws Exception {
		if (!isCollection()) {
			throw new Exception(
					"attempting to get collection for single object type");
		} else if (getCqlType() != DataType.Name.LIST) {
			throw new Exception("invalid bind: attempting to bind List for "
					+ getCqlType());
		}
		switch (getCollectionType()) {
		case BLOB: {
			List<ByteBuffer> list = new ArrayList<ByteBuffer>();
			for (String val : inList) {
				list.add(ByteBuffer.wrap(val.getBytes()));
			}
			return list;
		}
		case DECIMAL: {
			List<BigDecimal> list = new ArrayList<BigDecimal>();
			for (String val : inList) {
				list.add(new BigDecimal(val));
			}
			return list;
		}
		case VARINT: {
			List<BigInteger> list = new ArrayList<BigInteger>();
			for (String val : inList) {
				list.add(new BigInteger(val));
			}
			return list;
		}
		case BOOLEAN: {
			List<Boolean> list = new ArrayList<Boolean>();
			for (String val : inList) {
				list.add(Boolean.valueOf(val));
			}
			return list;
		}
		case INET: {
			List<InetAddress> list = new ArrayList<InetAddress>();
			for (String val : inList) {
				list.add(InetAddress.getByName(val));
			}
			return list;
		}
		case INT: {
			List<Integer> list = new ArrayList<Integer>();
			for (String val : inList) {
				list.add(Integer.valueOf(val));
			}
			return list;
		}
		case BIGINT:
		case COUNTER: {
			List<Long> list = new ArrayList<Long>();
			for (String val : inList) {
				list.add(Long.valueOf(val));
			}
			return list;
		}
		case FLOAT: {
			List<Float> list = new ArrayList<Float>();
			for (String val : inList) {
				list.add(Float.valueOf(val));
			}
			return list;
		}
		case DOUBLE: {
			List<Double> list = new ArrayList<Double>();
			for (String val : inList) {
				list.add(Double.valueOf(val));
			}
			return list;
		}
		case TIMESTAMP: {
			List<Date> list = new ArrayList<Date>();
			for (String val : inList) {
				list.add(Date.valueOf(val));
			}
			return list;
		}
		case TIMEUUID:
		case UUID: {
			List<UUID> list = new ArrayList<UUID>();
			for (String val : inList) {
				list.add(UUID.fromString(val));
			}
			return list;
		}
		default:
			return inList;
		}
	}

	/**
	 * Bind Map to Cassandra BoundStatement
	 * 
	 * @param bs
	 * @param inMap
	 * @throws Exception
	 */
	public void bindMap(BoundStatement bs, Map<String, String> inMap)
			throws Exception {
		if (!isCollection()) {
			throw new Exception(
					"attempting to bind non-collection as collection");
		}
		LOG.trace("bindMap: entered with {}", inMap.toString());
		Map<String, ?> map = getMap(inMap);
		for (Integer pos : getPositions())
			bs.setMap(pos, map);
	}

	/**
	 * Bind Set to BoundStatement
	 * 
	 * @param bs
	 * @param inSet
	 * @throws Exception
	 */
	public void bindSet(BoundStatement bs, Set<String> inSet) throws Exception {
		if (!isCollection()) {
			throw new Exception(
					"attempting to bind non-collection as collection");
		}
		LOG.trace("bindSet: entered with {}", inSet.toString());
		Set<?> set = getSet(inSet);
		for (Integer pos : getPositions())
			bs.setSet(pos, set);
	}

	/**
	 * Bind List to BoundStatement.
	 * 
	 * @param bs
	 * @param inList
	 * @throws Exception
	 */
	public void bindList(BoundStatement bs, List<String> inList)
			throws Exception {
		if (!isCollection()) {
			throw new Exception(
					"attempting to bind non-collection as collection");
		}
		LOG.trace("bindList: entered with {}", inList.toString());
		List<?> list = getList(inList);
		for (Integer pos : getPositions())
			bs.setList(pos, list);
	}

	/**
	 * Bind Tuple to BoundStatement.
	 * 
	 * @param bs
	 * @param inList
	 * @throws Exception
	 */
	public void bindTuple(BoundStatement bs, List<Object> inList)
			throws Exception {
		if (!isCollection()) {
			throw new Exception(
					"attempting to bind non-collection as collection");
		}
		LOG.trace("bindTuple: entered with {}", inList.toString());
		TupleType tupleType = Utils.getTupleType(inList);
		TupleValue tv = tupleType.newValue(inList.toArray(new Object[inList
				.size()]));
		for (Integer pos : getPositions())
			bs.setTupleValue(pos, tv);
	}

	public String toString() {
		return "cqlType=" + cqlType + " key=" + key + " value=" + value
				+ " positions=" + getPositions().toString();
	}

	public boolean isEqual(CqlToken token) {
		return (getKey().equals(token.getKey())
				&& getCqlType() == token.getCqlType() && token
					.getCollectionType() == getCollectionType()) ? true : false;
	}

	/**
	 * @return the collectionType
	 */
	public DataType.Name getCollectionType() {
		return collectionType;
	}

	/**
	 * @param collectionType
	 *            the collectionType to set
	 */
	public void setCollectionType(DataType.Name collectionType) {
		this.collectionType = collectionType;
	}

}
