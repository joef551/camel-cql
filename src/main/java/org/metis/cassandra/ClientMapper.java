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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.PathMatcher;
import org.springframework.context.support.ApplicationObjectSupport;

/**
 * This class is defined in the application context. The CassandraComponent
 * class looks for this object in the application context and uses it to find a
 * suitable Client for a given URI's context-path.
 * 
 * @author jfernandez
 * 
 */
public class ClientMapper extends ApplicationObjectSupport {

	// This component's logger
	private static final Logger LOG = LoggerFactory
			.getLogger(ClientMapper.class);

	private final Map<String, Object> urlMap = new HashMap<String, Object>();

	// we're using ant-style pattern matching
	private PathMatcher pathMatcher = new AntPathMatcher();

	private Object defaultHandler;

	private Object rootHandler;

	private boolean lazyInitHandlers = false;

	private final Map<String, Object> handlerMap = new LinkedHashMap<String, Object>();

	public ClientMapper() {
	}

	/**
	 * Return the registered handlers as an unmodifiable Map, with the
	 * registered path as key and the handler object (or handler bean name in
	 * case of a lazy-init handler) as value.
	 * 
	 * @see #getDefaultHandler()
	 */
	public final Map<String, Object> getHandlerMap() {
		return Collections.unmodifiableMap(this.handlerMap);
	}

	/**
	 * Set the PathMatcher implementation to use for matching URL paths against
	 * registered URL patterns. Default is AntPathMatcher.
	 * 
	 * @see org.springframework.util.AntPathMatcher
	 */
	public void setPathMatcher(PathMatcher pathMatcher) {
		Assert.notNull(pathMatcher, "PathMatcher must not be null");
		this.pathMatcher = pathMatcher;
	}

	/**
	 * Return the PathMatcher implementation to use for matching URL paths
	 * against registered URL patterns.
	 */
	public PathMatcher getPathMatcher() {
		return this.pathMatcher;
	}

	/**
	 * Set the root handler for this handler mapping, that is, the handler to be
	 * registered for the root path ("/").
	 * <p>
	 * Default is <code>null</code>, indicating no root handler.
	 */
	public void setRootHandler(Object rootHandler) {
		this.rootHandler = rootHandler;
	}

	/**
	 * Return the root handler for this handler mapping (registered for "/"), or
	 * <code>null</code> if none.
	 */
	public Object getRootHandler() {
		return this.rootHandler;
	}

	/**
	 * Set the default handler for this handler mapping. This handler will be
	 * returned if no specific mapping was found.
	 * <p>
	 * Default is <code>null</code>, indicating no default handler.
	 */
	public void setDefaultHandler(Object defaultHandler) {
		this.defaultHandler = defaultHandler;
	}

	/**
	 * Return the default handler for this handler mapping, or <code>null</code>
	 * if none.
	 */
	public Object getDefaultHandler() {
		return this.defaultHandler;
	}

	/**
	 * Set whether to lazily initialize handlers. Only applicable to singleton
	 * handlers, as prototypes are always lazily initialized. Default is
	 * "false", as eager initialization allows for more efficiency through
	 * referencing the controller objects directly.
	 * <p>
	 * If you want to allow your controllers to be lazily initialized, make them
	 * "lazy-init" and set this flag to true. Just making them "lazy-init" will
	 * not work, as they are initialized through the references from the handler
	 * mapping in this case.
	 */
	public void setLazyInitHandlers(boolean lazyInitHandlers) {
		this.lazyInitHandlers = lazyInitHandlers;
	}

	/**
	 * Map URL paths to handler bean names. This is the typical way of
	 * configuring this HandlerMapping.
	 * <p>
	 * Supports direct URL matches and Ant-style pattern matches. For syntax
	 * details, see the {@link org.springframework.util.AntPathMatcher} javadoc.
	 * 
	 * @param mappings
	 *            properties with URLs as keys and bean names as values
	 * @see #setUrlMap
	 */
	public void setMappings(Properties mappings) {
		CollectionUtils.mergePropertiesIntoMap(mappings, this.urlMap);
	}

	/**
	 * Set a Map with URL paths as keys and handler beans (or handler bean
	 * names) as values. Convenient for population with bean references.
	 * <p>
	 * Supports direct URL matches and Ant-style pattern matches. For syntax
	 * details, see the {@link org.springframework.util.AntPathMatcher} javadoc.
	 * 
	 * @param urlMap
	 *            map with URLs as keys and beans as values
	 * @see #setMappings
	 */
	public void setUrlMap(Map<String, ?> urlMap) {
		this.urlMap.putAll(urlMap);
	}

	/**
	 * Allow Map access to the URL path mappings, with the option to add or
	 * override specific entries.
	 * <p>
	 * Useful for specifying entries directly, for example via "urlMap[myKey]".
	 * This is particularly useful for adding or overriding entries in child
	 * bean definitions.
	 */
	public Map<String, ?> getUrlMap() {
		return this.urlMap;
	}

	/**
	 * Calls the {@link #registerHandlers} method in addition to the
	 * superclass's initialization.
	 */
	@Override
	public void initApplicationContext() throws BeansException {
		registerHandlers(this.urlMap);
	}

	/**
	 * Register all handlers specified in the URL map for the corresponding
	 * paths.
	 * 
	 * @param urlMap
	 *            Map with URL paths as keys and handler beans or bean names as
	 *            values
	 * @throws BeansException
	 *             if a handler couldn't be registered
	 * @throws IllegalStateException
	 *             if there is a conflicting handler registered
	 */
	protected void registerHandlers(Map<String, Object> urlMap)
			throws BeansException, NullPointerException {

		if (urlMap.isEmpty()) {
			throw new NullPointerException("urlMap is null");
		} else if (urlMap.isEmpty()) {
			logger.warn("Neither 'urlMap' nor 'mappings' set on TopendRdbMapper");
		} else {
			for (Map.Entry<String, Object> entry : urlMap.entrySet()) {
				String url = entry.getKey();
				Object handler = entry.getValue();
				// Prepend with slash if not already present.
				if (!url.startsWith("/")) {
					url = "/" + url;
				}
				// Remove whitespace from handler bean name.
				if (handler instanceof String) {
					handler = ((String) handler).trim();
				}
				registerHandler(url, handler);
			}
		}
	}

	/**
	 * Register the specified handler for the given URL paths.
	 * 
	 * @param urlPaths
	 *            the URLs that the bean should be mapped to
	 * @param beanName
	 *            the name of the handler bean
	 * @throws BeansException
	 *             if the handler couldn't be registered
	 * @throws IllegalStateException
	 *             if there is a conflicting handler registered
	 */
	protected void registerHandler(String[] urlPaths, String beanName)
			throws BeansException, IllegalStateException, NullPointerException {
		if (urlPaths == null) {
			throw new NullPointerException("urlPaths is null");
		} else if (beanName == null) {
			throw new NullPointerException("beanName is null");
		}
		for (String urlPath : urlPaths) {
			registerHandler(urlPath, beanName);
		}
	}

	/**
	 * Register the specified handler for the given URL path.
	 * 
	 * @param urlPath
	 *            the URL the bean should be mapped to
	 * @param handler
	 *            the handler instance or handler bean name String (a bean name
	 *            will automatically be resolved into the corresponding handler
	 *            bean)
	 * @throws BeansException
	 *             if the handler couldn't be registered
	 * @throws IllegalStateException
	 *             if there is a conflicting handler registered
	 */
	protected void registerHandler(String urlPath, Object handler)
			throws BeansException, IllegalStateException {
		Assert.notNull(urlPath, "URL path must not be null");
		Assert.notNull(handler, "Handler object must not be null");
		Object resolvedHandler = handler;

		// Eagerly resolve handler if referencing singleton via name.
		if (!this.lazyInitHandlers && handler instanceof String) {
			String handlerName = (String) handler;
			if (getApplicationContext().isSingleton(handlerName)) {
				resolvedHandler = getApplicationContext().getBean(handlerName);
			}
		}

		Object mappedHandler = this.handlerMap.get(urlPath);
		if (mappedHandler != null) {
			if (mappedHandler != resolvedHandler) {
				throw new IllegalStateException("Cannot map "
						+ getHandlerDescription(handler) + " to URL path ["
						+ urlPath + "]: There is already "
						+ getHandlerDescription(mappedHandler) + " mapped.");
			}
		} else {
			if (urlPath.equals("/")) {
				if (LOG.isInfoEnabled()) {
					LOG.info("Root mapping to "
							+ getHandlerDescription(handler));
				}
				setRootHandler(resolvedHandler);
			} else if (urlPath.equals("/*")) {
				if (LOG.isInfoEnabled()) {
					LOG.info("Default mapping to "
							+ getHandlerDescription(handler));
				}
				setDefaultHandler(resolvedHandler);
			} else {
				this.handlerMap.put(urlPath, resolvedHandler);
				if (LOG.isInfoEnabled()) {
					LOG.info("Mapped URL path [" + urlPath + "] onto "
							+ getHandlerDescription(handler));
				}
			}
		}
	}

	/**
	 * Look up a TopendResourceBean (handler) instance for the given URL path.
	 * <p>
	 * A handler supports direct matches, e.g. a registered "/test" matches
	 * "/test", and various Ant-style pattern matches, e.g. a registered "/t*"
	 * matches both "/test" and "/team". For details, see the AntPathMatcher
	 * class.
	 * <p>
	 * Looks for the most exact pattern, where most exact is defined as the
	 * longest path pattern.
	 * 
	 * @param urlPath
	 *            URL the bean is mapped to
	 * @return the associated handler instance, or <code>null</code> if not
	 *         found
	 * @see #exposePathWithinMapping
	 * @see org.springframework.util.AntPathMatcher
	 */
	protected Object lookupHandler(String urlPath) throws Exception {

		if (LOG.isTraceEnabled()) {
			LOG.trace("lookupHandler: context path = " + urlPath);
		}

		// See if we have a direct match
		Object handler = getHandlerMap().get(urlPath);
		if (handler != null) {
			// Bean name or resolved handler?
			if (handler instanceof String) {
				String handlerName = (String) handler;
				handler = getApplicationContext().getBean(handlerName);
			}
			if (LOG.isTraceEnabled()) {
				LOG.trace("lookupHandler: direct find");
			}
			return handler;
		}

		// No direct match, look for a pattern match
		List<String> matchingPatterns = new ArrayList<String>();
		for (String registeredPattern : getHandlerMap().keySet()) {
			if (getPathMatcher().match(registeredPattern, urlPath)) {
				matchingPatterns.add(registeredPattern);
			}
		}

		if (LOG.isTraceEnabled()) {
			LOG.trace("lookupHandler: found this many matching patterns = "
					+ matchingPatterns.size());
		}

		String bestPatternMatch = null;
		Comparator<String> patternComparator = getPathMatcher()
				.getPatternComparator(urlPath);
		if (!matchingPatterns.isEmpty()) {
			Collections.sort(matchingPatterns, patternComparator);
			if (LOG.isTraceEnabled()) {
				LOG.trace("Matching patterns for request [" + urlPath
						+ "] are " + matchingPatterns);
			}
			bestPatternMatch = matchingPatterns.get(0);
		}
		if (bestPatternMatch != null) {
			handler = this.getHandlerMap().get(bestPatternMatch);
			// Bean name or resolved handler?
			if (handler instanceof String) {
				String handlerName = (String) handler;
				handler = getApplicationContext().getBean(handlerName);
			}
			return handler;

		}
		// No handler found...
		return null;
	}

	private String getHandlerDescription(Object handler) {
		return "handler "
				+ (handler instanceof String ? "'" + handler + "'"
						: "of type [" + handler.getClass() + "]");
	}

}
