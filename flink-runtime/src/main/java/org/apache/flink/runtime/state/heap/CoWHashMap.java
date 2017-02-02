/*
 * Copyright (c) 1997, 2013, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package org.apache.flink.runtime.state.heap;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class CoWHashMap<K, N, V> {//implements Map<K, V> {

	/**
	 * The default initial capacity - MUST be a power of two.
	 */
	static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

	/**
	 * The maximum capacity, used if a higher value is implicitly specified
	 * by either of the constructors with arguments.
	 * MUST be a power of two <= 1<<30.
	 */
	static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * The load factor used when none specified in constructor.
	 */
	static final float DEFAULT_LOAD_FACTOR = 0.75f;

	/**
	 * The bin count threshold for using a tree rather than list for a
	 * bin.  Bins are converted to trees when adding an element to a
	 * bin with at least this many nodes. The value must be greater
	 * than 2 and should be at least 8 to mesh with assumptions in
	 * tree removal about conversion back to plain bins upon
	 * shrinkage.
	 */
	static final int TREEIFY_THRESHOLD = 8;

	/**
	 * The bin count threshold for untreeifying a (split) bin during a
	 * resize operation. Should be less than TREEIFY_THRESHOLD, and at
	 * most 6 to mesh with shrinkage detection under removal.
	 */
	static final int UNTREEIFY_THRESHOLD = 6;

	/**
	 * The smallest table capacity for which bins may be treeified.
	 * (Otherwise the table is resized if too many nodes in a bin.)
	 * Should be at least 4 * TREEIFY_THRESHOLD to avoid conflicts
	 * between resizing and treeification thresholds.
	 */
	static final int MIN_TREEIFY_CAPACITY = 64;

	/**
	 * Basic hash bin node, used for most entries.  (See below for
	 * TreeNode subclass, and in LinkedHashMap for its Entry subclass.)
	 */
	static class Node<K, N, V> {
		final int hash;
		final K key;
		final N namespace;
		V value;
		int version;

		Node<K, N, V> next;

		Node(int hash, K key, N namespace, V value, Node<K, N, V> next) {
			this.hash = hash;
			this.key = key;
			this.namespace = namespace;
			this.value = value;
			this.next = next;
		}

		public final K getKey() {
			return key;
		}

		public N getNamespace() {
			return namespace;
		}

		public final V getValue() {
			return value;
		}

		public final String toString() {
			return "(" + key + "|" + namespace + ")=" + value;
		}

		public final int hashCode() {
			return Objects.hashCode(key) ^ Objects.hashCode(value);
		}

		public final V setValue(V newValue) {
			V oldValue = value;
			value = newValue;
			return oldValue;
		}

		public final boolean equals(Object o) {
			if (o == this)
				return true;
			if (o instanceof Node) {
				Node<?, ?, ?> e = (Node<?, ?, ?>) o;
				if (Objects.equals(key, e.getKey()) &&
						Objects.equals(namespace, e.getNamespace()) &&
						Objects.equals(value, e.getValue()))
					return true;
			}
			return false;
		}
	}

    /* ---------------- Static utilities -------------- */

	/**
	 * Computes key.hashCode() and spreads (XORs) higher bits of hash
	 * to lower.  Because the table uses power-of-two masking, sets of
	 * hashes that vary only in bits above the current mask will
	 * always collide. (Among known examples are sets of Float keys
	 * holding consecutive whole numbers in small tables.)  So we
	 * apply a transform that spreads the impact of higher bits
	 * downward. There is a tradeoff between speed, utility, and
	 * quality of bit-spreading. Because many common sets of hashes
	 * are already reasonably distributed (so don't benefit from
	 * spreading), and because we use trees to handle large sets of
	 * collisions in bins, we just XOR some shifted bits in the
	 * cheapest possible way to reduce systematic lossage, as well as
	 * to incorporate impact of the highest bits that would otherwise
	 * never be used in index calculations because of table bounds.
	 */
	static final int hash(Object key, Object namespace) {
		int h;
		return (h = 31 * key.hashCode() + namespace.hashCode()) ^ (h >>> 16);
	}

	/**
	 * Returns x's Class if it is of the form "class C implements
	 * Comparable<C>", else null.
	 */
	static Class<?> comparableClassFor(Object x) {
		if (x instanceof Comparable) {
			Class<?> c;
			Type[] ts, as;
			Type t;
			ParameterizedType p;
			if ((c = x.getClass()) == String.class) // bypass checks
				return c;
			if ((ts = c.getGenericInterfaces()) != null) {
				for (int i = 0; i < ts.length; ++i) {
					if (((t = ts[i]) instanceof ParameterizedType) &&
							((p = (ParameterizedType) t).getRawType() ==
									Comparable.class) &&
							(as = p.getActualTypeArguments()) != null &&
							as.length == 1 && as[0] == c) // type arg is c
						return c;
				}
			}
		}
		return null;
	}

	/**
	 * Returns k.compareTo(x) if x matches kc (k's screened comparable
	 * class), else 0.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"}) // for cast to Comparable
	static int compareComparables(Class<?> kc, Class<?> nc, Object k, Object kx, Object n, Object nx) {

		int cmp = (nx == null || nx.getClass() != nc ? 0 :
				((Comparable) n).compareTo(nx));

		return cmp != 0 ? cmp : (kx == null || kx.getClass() != kc ? 0 :
				((Comparable) k).compareTo(kx));

	}

	/**
	 * Returns a power of two size for the given target capacity.
	 */
	static final int tableSizeFor(int cap) {
		int n = cap - 1;
		n |= n >>> 1;
		n |= n >>> 2;
		n |= n >>> 4;
		n |= n >>> 8;
		n |= n >>> 16;
		return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
	}

    /* ---------------- Fields -------------- */

	/**
	 * The table, initialized on first use, and resized as
	 * necessary. When allocated, length is always a power of two.
	 * (We also tolerate length zero in some operations to allow
	 * bootstrapping mechanics that are currently not needed.)
	 */
	transient Node<K, N, V>[] table;

	/**
	 * The number of key-value mappings contained in this map.
	 */
	transient int size;

	/**
	 * The number of times this HashMap has been structurally modified
	 * Structural modifications are those that change the number of mappings in
	 * the HashMap or otherwise modify its internal structure (e.g.,
	 * rehash).  This field is used to make iterators on Collection-views of
	 * the HashMap fail-fast.  (See ConcurrentModificationException).
	 */
	transient int modCount;

	/**
	 * The next size value at which to resize (capacity * load factor).
	 *
	 * @serial
	 */
	// (The javadoc description is true upon serialization.
	// Additionally, if the table array has not been allocated, this
	// field holds the initial array capacity, or zero signifying
	// DEFAULT_INITIAL_CAPACITY.)
	int threshold;

	/**
	 * The load factor for the hash table.
	 *
	 * @serial
	 */
	final float loadFactor;

    /* ---------------- Public operations -------------- */

	/**
	 * Constructs an empty <tt>HashMap</tt> with the specified initial
	 * capacity and load factor.
	 *
	 * @param initialCapacity the initial capacity
	 * @param loadFactor      the load factor
	 * @throws IllegalArgumentException if the initial capacity is negative
	 *                                  or the load factor is nonpositive
	 */
	public CoWHashMap(int initialCapacity, float loadFactor) {
		if (initialCapacity < 0)
			throw new IllegalArgumentException("Illegal initial capacity: " +
					initialCapacity);
		if (initialCapacity > MAXIMUM_CAPACITY)
			initialCapacity = MAXIMUM_CAPACITY;
		if (loadFactor <= 0 || Float.isNaN(loadFactor))
			throw new IllegalArgumentException("Illegal load factor: " +
					loadFactor);
		this.loadFactor = loadFactor;
		this.threshold = tableSizeFor(initialCapacity);
	}

	/**
	 * Constructs an empty <tt>HashMap</tt> with the specified initial
	 * capacity and the default load factor (0.75).
	 *
	 * @param initialCapacity the initial capacity.
	 * @throws IllegalArgumentException if the initial capacity is negative.
	 */
	public CoWHashMap(int initialCapacity) {
		this(initialCapacity, DEFAULT_LOAD_FACTOR);
	}

	/**
	 * Constructs an empty <tt>HashMap</tt> with the default initial capacity
	 * (16) and the default load factor (0.75).
	 */
	public CoWHashMap() {
		this.loadFactor = DEFAULT_LOAD_FACTOR; // all other fields defaulted
	}

//	/**
//	 * Constructs a new <tt>HashMap</tt> with the same mappings as the
//	 * specified <tt>Map</tt>.  The <tt>HashMap</tt> is created with
//	 * default load factor (0.75) and an initial capacity sufficient to
//	 * hold the mappings in the specified <tt>Map</tt>.
//	 *
//	 * @param m the map whose mappings are to be placed in this map
//	 * @throws NullPointerException if the specified map is null
//	 */
//	public CoWHashMap(Map<? extends K, ? extends V> m) {
//		this.loadFactor = DEFAULT_LOAD_FACTOR;
//		putMapEntries(m, false);
//	}
//
//	/**
//	 * Implements Map.putAll and Map constructor
//	 *
//	 * @param m     the map
//	 * @param evict false when initially constructing this map, else
//	 *              true (relayed to method afterNodeInsertion).
//	 */
//	final void putMapEntries(CoWHashMap<? extends K, ? extends N, ? extends V> m, boolean evict) {
//		int s = m.size();
//		if (s > 0) {
//			if (table == null) { // pre-size
//				float ft = ((float) s / loadFactor) + 1.0F;
//				int t = ((ft < (float) MAXIMUM_CAPACITY) ?
//						(int) ft : MAXIMUM_CAPACITY);
//				if (t > threshold)
//					threshold = tableSizeFor(t);
//			} else if (s > threshold)
//				resize();
//			for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
//				K key = e.getKey();
//				N namespace = e.
//				V value = e.getValue();
//				putVal(hash(key), key, value, false, evict);
//			}
//		}
//	}

	/**
	 * Returns the number of key-value mappings in this map.
	 *
	 * @return the number of key-value mappings in this map
	 */
	public int size() {
		return size;
	}

	/**
	 * Returns <tt>true</tt> if this map contains no key-value mappings.
	 *
	 * @return <tt>true</tt> if this map contains no key-value mappings
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	public V get(Object key, Object namespace) {
		Node<K, N, V> e;
		return (e = getNode(hash(key, namespace), key, namespace)) == null ? null : e.value;
	}

	/**
	 * Implements Map.get and related methods
	 *
	 * @param hash hash for key
	 * @param key  the key
	 * @return the node, or null if none
	 */
	final Node<K, N, V> getNode(int hash, Object key, Object namespace) {
		Node<K, N, V>[] tab;
		Node<K, N, V> first, e;
		int n;
		K k;
		N ns;
		if ((tab = table) != null && (n = tab.length) > 0 &&
				(first = tab[(n - 1) & hash]) != null) {
			if (first.hash == hash && // always check first node
					((k = first.key) == key || (key != null && key.equals(k))) &&
					((ns = first.namespace) == namespace || (namespace != null && namespace.equals(ns))))
				return first;
			if ((e = first.next) != null) {
				if (first instanceof TreeNode)
					return ((TreeNode<K, N, V>) first).getTreeNode(hash, key, namespace);
				do {
					if (e.hash == hash &&
							((k = e.key) == key || (key != null && key.equals(k))) &&
							((ns = e.namespace) == namespace || (namespace != null && namespace.equals(ns))))
						return e;
				} while ((e = e.next) != null);
			}
		}
		return null;
	}

	/**
	 * Returns <tt>true</tt> if this map contains a mapping for the
	 * specified key.
	 *
	 * @param key The key whose presence in this map is to be tested
	 * @return <tt>true</tt> if this map contains a mapping for the specified
	 * key.
	 */
	public boolean containsKey(Object key, Object namespace) {
		return getNode(hash(key, namespace), key, namespace) != null;
	}

	/**
	 * Associates the specified value with the specified key in this map.
	 * If the map previously contained a mapping for the key, the old
	 * value is replaced.
	 *
	 * @param key   key with which the specified value is to be associated
	 * @param value value to be associated with the specified key
	 * @return the previous value associated with <tt>key</tt>, or
	 * <tt>null</tt> if there was no mapping for <tt>key</tt>.
	 * (A <tt>null</tt> return can also indicate that the map
	 * previously associated <tt>null</tt> with <tt>key</tt>.)
	 */
	public V put(K key, N namespace, V value) {
		return putVal(hash(key, namespace), key, namespace, value, false, true);
	}

	/**
	 * Implements Map.put and related methods
	 *
	 * @param hash         hash for key
	 * @param key          the key
	 * @param value        the value to put
	 * @param onlyIfAbsent if true, don't change existing value
	 * @param evict        if false, the table is in creation mode.
	 * @return previous value, or null if none
	 */
	final V putVal(int hash, K key, N namespace, V value, boolean onlyIfAbsent, boolean evict) {
		Node<K, N, V>[] tab;
		Node<K, N, V> p;
		int n, i;
		if ((tab = table) == null || (n = tab.length) == 0)
			n = (tab = resize()).length;
		if ((p = tab[i = (n - 1) & hash]) == null)
			tab[i] = newNode(hash, key, namespace, value, null);
		else {
			Node<K, N, V> e;
			K k;
			N ns;
			if (p.hash == hash &&
					((k = p.key) == key || (key != null && key.equals(k))) &&
					((ns = p.namespace) == namespace || (namespace != null && namespace.equals(ns))))
				e = p;
			else if (p instanceof TreeNode)
				e = ((TreeNode<K, N, V>) p).putTreeVal(this, tab, hash, key, namespace, value);
			else {
				for (int binCount = 0; ; ++binCount) {
					if ((e = p.next) == null) {
						p.next = newNode(hash, key, namespace, value, null);
						if (binCount >= TREEIFY_THRESHOLD - 1) // -1 for 1st
							treeifyBin(tab, hash);
						break;
					}
					if (e.hash == hash &&
							((k = e.key) == key || (key != null && key.equals(k))) &&
							((ns = e.namespace) == namespace || (namespace != null && namespace.equals(ns))))
						break;
					p = e;
				}
			}
			if (e != null) { // existing mapping for key
				V oldValue = e.value;
				if (!onlyIfAbsent || oldValue == null)
					e.value = value;
				afterNodeAccess(e);
				return oldValue;
			}
		}
		++modCount;
		if (++size > threshold)
			resize();
		afterNodeInsertion(evict);
		return null;
	}

	/**
	 * Initializes or doubles table size.  If null, allocates in
	 * accord with initial capacity target held in field threshold.
	 * Otherwise, because we are using power-of-two expansion, the
	 * elements from each bin must either stay at same index, or move
	 * with a power of two offset in the new table.
	 *
	 * @return the table
	 */
	final Node<K, N, V>[] resize() {
		Node<K, N, V>[] oldTab = table;
		int oldCap = (oldTab == null) ? 0 : oldTab.length;
		int oldThr = threshold;
		int newCap, newThr = 0;
		if (oldCap > 0) {
			if (oldCap >= MAXIMUM_CAPACITY) {
				threshold = Integer.MAX_VALUE;
				return oldTab;
			} else if ((newCap = oldCap << 1) < MAXIMUM_CAPACITY &&
					oldCap >= DEFAULT_INITIAL_CAPACITY)
				newThr = oldThr << 1; // double threshold
		} else if (oldThr > 0) // initial capacity was placed in threshold
			newCap = oldThr;
		else {               // zero initial threshold signifies using defaults
			newCap = DEFAULT_INITIAL_CAPACITY;
			newThr = (int) (DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
		}
		if (newThr == 0) {
			float ft = (float) newCap * loadFactor;
			newThr = (newCap < MAXIMUM_CAPACITY && ft < (float) MAXIMUM_CAPACITY ?
					(int) ft : Integer.MAX_VALUE);
		}
		threshold = newThr;
		@SuppressWarnings({"rawtypes", "unchecked"})
		Node<K, N, V>[] newTab = (Node<K, N, V>[]) new Node[newCap];
		table = newTab;
		if (oldTab != null) {
			for (int j = 0; j < oldCap; ++j) {
				Node<K, N, V> e;
				if ((e = oldTab[j]) != null) {
					oldTab[j] = null;
					if (e.next == null)
						newTab[e.hash & (newCap - 1)] = e;
					else if (e instanceof TreeNode)
						((TreeNode<K, N, V>) e).split(this, newTab, j, oldCap);
					else { // preserve order
						Node<K, N, V> loHead = null, loTail = null;
						Node<K, N, V> hiHead = null, hiTail = null;
						Node<K, N, V> next;
						do {
							next = e.next;
							if ((e.hash & oldCap) == 0) {
								if (loTail == null)
									loHead = e;
								else
									loTail.next = e;
								loTail = e;
							} else {
								if (hiTail == null)
									hiHead = e;
								else
									hiTail.next = e;
								hiTail = e;
							}
						} while ((e = next) != null);
						if (loTail != null) {
							loTail.next = null;
							newTab[j] = loHead;
						}
						if (hiTail != null) {
							hiTail.next = null;
							newTab[j + oldCap] = hiHead;
						}
					}
				}
			}
		}
		return newTab;
	}

	/**
	 * Replaces all linked nodes in bin at index for given hash unless
	 * table is too small, in which case resizes instead.
	 */
	final void treeifyBin(Node<K, N, V>[] tab, int hash) {
		int n, index;
		Node<K, N, V> e;
		if (tab == null || (n = tab.length) < MIN_TREEIFY_CAPACITY)
			resize();
		else if ((e = tab[index = (n - 1) & hash]) != null) {
			TreeNode<K, N, V> hd = null, tl = null;
			do {
				TreeNode<K, N, V> p = replacementTreeNode(e, null);
				if (tl == null)
					hd = p;
				else {
					p.prev = tl;
					tl.next = p;
				}
				tl = p;
			} while ((e = e.next) != null);
			if ((tab[index] = hd) != null)
				hd.treeify(tab);
		}
	}

	/**
	 * Removes the mapping for the specified key from this map if present.
	 *
	 * @param key key whose mapping is to be removed from the map
	 * @return the previous value associated with <tt>key</tt>, or
	 * <tt>null</tt> if there was no mapping for <tt>key</tt>.
	 * (A <tt>null</tt> return can also indicate that the map
	 * previously associated <tt>null</tt> with <tt>key</tt>.)
	 */
	public V remove(Object key, Object namespace) {
		Node<K, N, V> e;
		return (e = removeNode(hash(key, namespace), key, namespace, null, false, true)) == null ?
				null : e.value;
	}

	/**
	 * Implements Map.remove and related methods
	 *
	 * @param hash       hash for key
	 * @param key        the key
	 * @param value      the value to match if matchValue, else ignored
	 * @param matchValue if true only remove if value is equal
	 * @param movable    if false do not move other nodes while removing
	 * @return the node, or null if none
	 */
	final Node<K, N, V> removeNode(int hash, Object key, Object namespace, Object value,
	                               boolean matchValue, boolean movable) {
		Node<K, N, V>[] tab;
		Node<K, N, V> p;
		int n, index;
		if ((tab = table) != null && (n = tab.length) > 0 &&
				(p = tab[index = (n - 1) & hash]) != null) {
			Node<K, N, V> node = null, e;
			K k;
			N ns;
			V v;
			if (p.hash == hash &&
					((k = p.key) == key || (key != null && key.equals(k))) &&
					((ns = p.namespace) == namespace || (namespace != null && namespace.equals(ns))))
				node = p;
			else if ((e = p.next) != null) {
				if (p instanceof TreeNode)
					node = ((TreeNode<K, N, V>) p).getTreeNode(hash, key, namespace);
				else {
					do {
						if (e.hash == hash &&
								((k = e.key) == key || (key != null && key.equals(k))) &&
								((ns = e.namespace) == namespace || (namespace != null && namespace.equals(ns)))) {
							node = e;
							break;
						}
						p = e;
					} while ((e = e.next) != null);
				}
			}
			if (node != null && (!matchValue || (v = node.value) == value ||
					(value != null && value.equals(v)))) {
				if (node instanceof TreeNode)
					((TreeNode<K, N, V>) node).removeTreeNode(this, tab, movable);
				else if (node == p)
					tab[index] = node.next;
				else
					p.next = node.next;
				++modCount;
				--size;
				afterNodeRemoval(node);
				return node;
			}
		}
		return null;
	}

	/**
	 * Removes all of the mappings from this map.
	 * The map will be empty after this call returns.
	 */
	public void clear() {
		Node<K, N, V>[] tab;
		modCount++;
		if ((tab = table) != null && size > 0) {
			size = 0;
			for (int i = 0; i < tab.length; ++i)
				tab[i] = null;
		}
	}

	/**
	 * Returns <tt>true</tt> if this map maps one or more keys to the
	 * specified value.
	 *
	 * @param value value whose presence in this map is to be tested
	 * @return <tt>true</tt> if this map maps one or more keys to the
	 * specified value
	 */
	public boolean containsValue(Object value) {
		Node<K, N, V>[] tab;
		V v;
		if ((tab = table) != null && size > 0) {
			for (int i = 0; i < tab.length; ++i) {
				for (Node<K, N, V> e = tab[i]; e != null; e = e.next) {
					if ((v = e.value) == value ||
							(value != null && value.equals(v)))
						return true;
				}
			}
		}
		return false;
	}

	public Set<K> keySet() {
		throw new UnsupportedOperationException();
	}

	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

	public Set<Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

/* ------------------------------------------------------------ */
//	// Cloning and serialization
//
//	/**
//	 * Returns a shallow copy of this <tt>HashMap</tt> instance: the keys and
//	 * values themselves are not cloned.
//	 *
//	 * @return a shallow copy of this map
//	 */
//	@SuppressWarnings("unchecked")
//	@Override
//	public Object clone() {
//		CoWHashMap<K,V> result;
//		try {
//			result = (CoWHashMap<K,V>)super.clone();
//		} catch (CloneNotSupportedException e) {
//			// this shouldn't happen, since we are Cloneable
//			throw new AssertionError(e);
//		}
//		result.reinitialize();
//		result.putMapEntries(this, false);
//		return result;
//	}
//
//	// These methods are also used when serializing HashSets
//	final float loadFactor() { return loadFactor; }
//	final int capacity() {
//		return (table != null) ? table.length :
//				(threshold > 0) ? threshold :
//						DEFAULT_INITIAL_CAPACITY;
//	}
//
//	/**
//	 * Save the state of the <tt>HashMap</tt> instance to a stream (i.e.,
//	 * serialize it).
//	 *
//	 * @serialData The <i>capacity</i> of the HashMap (the length of the
//	 *             bucket array) is emitted (int), followed by the
//	 *             <i>size</i> (an int, the number of key-value
//	 *             mappings), followed by the key (Object) and value (Object)
//	 *             for each key-value mapping.  The key-value mappings are
//	 *             emitted in no particular order.
//	 */
//	private void writeObject(java.io.ObjectOutputStream s)
//			throws IOException {
//		int buckets = capacity();
//		// Write out the threshold, loadfactor, and any hidden stuff
//		s.defaultWriteObject();
//		s.writeInt(buckets);
//		s.writeInt(size);
//		internalWriteEntries(s);
//	}
//
//	/**
//	 * Reconstitute the {@code HashMap} instance from a stream (i.e.,
//	 * deserialize it).
//	 */
//	private void readObject(java.io.ObjectInputStream s)
//			throws IOException, ClassNotFoundException {
//		// Read in the threshold (ignored), loadfactor, and any hidden stuff
//		s.defaultReadObject();
//		reinitialize();
//		if (loadFactor <= 0 || Float.isNaN(loadFactor))
//			throw new InvalidObjectException("Illegal load factor: " +
//					loadFactor);
//		s.readInt();                // Read and ignore number of buckets
//		int mappings = s.readInt(); // Read number of mappings (size)
//		if (mappings < 0)
//			throw new InvalidObjectException("Illegal mappings count: " +
//					mappings);
//		else if (mappings > 0) { // (if zero, use defaults)
//			// Size the table using given load factor only if within
//			// range of 0.25...4.0
//			float lf = Math.min(Math.max(0.25f, loadFactor), 4.0f);
//			float fc = (float)mappings / lf + 1.0f;
//			int cap = ((fc < DEFAULT_INITIAL_CAPACITY) ?
//					DEFAULT_INITIAL_CAPACITY :
//					(fc >= MAXIMUM_CAPACITY) ?
//							MAXIMUM_CAPACITY :
//							tableSizeFor((int)fc));
//			float ft = (float)cap * lf;
//			threshold = ((cap < MAXIMUM_CAPACITY && ft < MAXIMUM_CAPACITY) ?
//					(int)ft : Integer.MAX_VALUE);
//			@SuppressWarnings({"rawtypes","unchecked"})
//			Node<K,V>[] tab = (Node<K,V>[])new Node[cap];
//			table = tab;
//
//			// Read the keys and values, and put the mappings in the HashMap
//			for (int i = 0; i < mappings; i++) {
//				@SuppressWarnings("unchecked")
//				K key = (K) s.readObject();
//				@SuppressWarnings("unchecked")
//				V value = (V) s.readObject();
//				putVal(hash(key), key, value, false, false);
//			}
//		}
//	}

    /* ------------------------------------------------------------ */
	// iterators

//	abstract class HashIterator {
//		Node<K, V> next;        // next entry to return
//		Node<K, V> current;     // current entry
//		int expectedModCount;  // for fast-fail
//		int index;             // current slot
//
//		HashIterator() {
//			expectedModCount = modCount;
//			Node<K, V>[] t = table;
//			current = next = null;
//			index = 0;
//			if (t != null && size > 0) { // advance to first entry
//				do {
//				} while (index < t.length && (next = t[index++]) == null);
//			}
//		}
//
//		public final boolean hasNext() {
//			return next != null;
//		}
//
//		final Node<K, V> nextNode() {
//			Node<K, V>[] t;
//			Node<K, V> e = next;
//			if (modCount != expectedModCount)
//				throw new ConcurrentModificationException();
//			if (e == null)
//				throw new NoSuchElementException();
//			if ((next = (current = e).next) == null && (t = table) != null) {
//				do {
//				} while (index < t.length && (next = t[index++]) == null);
//			}
//			return e;
//		}
//
//		public final void remove() {
//			Node<K, V> p = current;
//			if (p == null)
//				throw new IllegalStateException();
//			if (modCount != expectedModCount)
//				throw new ConcurrentModificationException();
//			current = null;
//			K key = p.key;
//			removeNode(hash(key), key, null, false, false);
//			expectedModCount = modCount;
//		}
//	}

    /* ------------------------------------------------------------ */
	// LinkedHashMap support


    /*
     * The following package-protected methods are designed to be
     * overridden by LinkedHashMap, but not by any other subclass.
     * Nearly all other internal methods are also package-protected
     * but are declared final, so can be used by LinkedHashMap, view
     * classes, and HashSet.
     */

	// Create a regular (non-tree) node
	Node<K, N, V> newNode(int hash, K key, N namespace, V value, Node<K, N, V> next) {
		return new Node<>(hash, key, namespace, value, next);
	}

	// For conversion from TreeNodes to plain nodes
	Node<K, N, V> replacementNode(Node<K, N, V> p, Node<K, N, V> next) {
		return new Node<>(p.hash, p.key, p.namespace, p.value, next);
	}

	// Create a tree bin node
	TreeNode<K, N, V> newTreeNode(int hash, K key, N namespace, V value, Node<K, N, V> next) {
		return new TreeNode<>(hash, key, namespace, value, next);
	}

	// For treeifyBin
	TreeNode<K, N, V> replacementTreeNode(Node<K, N, V> p, Node<K, N, V> next) {
		return new TreeNode<>(p.hash, p.key, p.namespace, p.value, next);
	}

	// Callbacks to allow LinkedHashMap post-actions
	void afterNodeAccess(Node<K, N, V> p) {
	}

	void afterNodeInsertion(boolean evict) {
	}

	void afterNodeRemoval(Node<K, N, V> p) {
	}

//	// Called only from writeObject, to ensure compatible ordering.
//	void internalWriteEntries(java.io.ObjectOutputStream s) throws IOException {
//		Node<K, N, V>[] tab;
//		if (size > 0 && (tab = table) != null) {
//			for (int i = 0; i < tab.length; ++i) {
//				for (Node<K, V> e = tab[i]; e != null; e = e.next) {
//					s.writeObject(e.key);
//					s.writeObject(e.value);
//				}
//			}
//		}
//	}

    /* ------------------------------------------------------------ */
	// Tree bins

	/**
	 * Entry for Tree bins. Extends LinkedHashMap.Entry (which in turn
	 * extends Node) so can be used as extension of either regular or
	 * linked node.
	 */
	static final class TreeNode<K, N, V> extends Node<K, N, V> {
		TreeNode<K, N, V> parent;  // red-black tree links
		TreeNode<K, N, V> left;
		TreeNode<K, N, V> right;
		TreeNode<K, N, V> prev;    // needed to unlink next upon deletion
		boolean red;

		TreeNode(int hash, K key, N namespace, V val, Node<K, N, V> next) {
			super(hash, key, namespace, val, next);
		}

		/**
		 * Returns root of tree containing this node.
		 */
		final TreeNode<K, N, V> root() {
			for (TreeNode<K, N, V> r = this, p; ; ) {
				if ((p = r.parent) == null)
					return r;
				r = p;
			}
		}

		/**
		 * Ensures that the given root is the first node of its bin.
		 */
		static <K, N, V> void moveRootToFront(Node<K, N, V>[] tab, TreeNode<K, N, V> root) {
			int n;
			if (root != null && tab != null && (n = tab.length) > 0) {
				int index = (n - 1) & root.hash;
				TreeNode<K, N, V> first = (TreeNode<K, N, V>) tab[index];
				if (root != first) {
					Node<K, N, V> rn;
					tab[index] = root;
					TreeNode<K, N, V> rp = root.prev;
					if ((rn = root.next) != null)
						((TreeNode<K, N, V>) rn).prev = rp;
					if (rp != null)
						rp.next = rn;
					if (first != null)
						first.prev = root;
					root.next = first;
					root.prev = null;
				}
				assert checkInvariants(root);
			}
		}

		/**
		 * Finds the node starting at root p with the given hash and key.
		 * The kc argument caches comparableClassFor(key) upon first use
		 * comparing keys.
		 */
		final TreeNode<K, N, V> find(int h, Object k, Object n, Class<?> kc, Class<?> nc) {
			TreeNode<K, N, V> p = this;
			do {
				int ph, dir;
				K pk;
				N pn = p.namespace;
				TreeNode<K, N, V> pl = p.left, pr = p.right, q;
				if ((ph = p.hash) > h)
					p = pl;
				else if (ph < h)
					p = pr;
				else if ((pk = p.key) == k || (k != null && k.equals(pk)) && (pn == n || (n != null && n.equals(pn))))
					return p;
				else if (pl == null)
					p = pr;
				else if (pr == null)
					p = pl;
				else if ((kc != null ||
						(kc = comparableClassFor(k)) != null &&
						(nc = comparableClassFor(n)) != null) &&
						(dir = compareComparables(kc, nc, k, pk, n, pn)) != 0)
					p = (dir < 0) ? pl : pr;
				else if ((q = pr.find(h, k, n, kc, nc)) != null)
					return q;
				else
					p = pl;
			} while (p != null);
			return null;
		}

		/**
		 * Calls find for root node.
		 */
		final TreeNode<K, N, V> getTreeNode(int h, Object k, Object n) {
			return ((parent != null) ? root() : this).find(h, k, n, null, null);
		}

		/**
		 * Tie-breaking utility for ordering insertions when equal
		 * hashCodes and non-comparable. We don't require a total
		 * order, just a consistent insertion rule to maintain
		 * equivalence across rebalancings. Tie-breaking further than
		 * necessary simplifies testing a bit.
		 */
		static int tieBreakOrder(Object a, Object b, Object x, Object y) {
			int d;

			if (a == null || b == null ||
					(d = a.getClass().getName().compareTo(b.getClass().getName())) == 0 && (d = x.getClass().getName().
							compareTo(y.getClass().getName())) == 0) {

				d = (System.identityHashCode(a) <= System.identityHashCode(b) ?
						-1 : 1);
			}
			return d;
		}

		/**
		 * Forms tree of the nodes linked from this node.
		 *
		 * @return root of tree
		 */
		final void treeify(Node<K, N, V>[] tab) {
			TreeNode<K, N, V> root = null;
			for (TreeNode<K, N, V> x = this, next; x != null; x = next) {
				next = (TreeNode<K, N, V>) x.next;
				x.left = x.right = null;
				if (root == null) {
					x.parent = null;
					x.red = false;
					root = x;
				} else {
					K k = x.key;
					N n = x.namespace;
					int h = x.hash;
					Class<?> kc = null;
					Class<?> nc = null;
					for (TreeNode<K, N, V> p = root; ; ) {
						int dir, ph;
						K pk = p.key;
						N pn = p.namespace;
						if ((ph = p.hash) > h)
							dir = -1;
						else if (ph < h)
							dir = 1;
						else if (((kc == null &&
								(kc = comparableClassFor(k)) == null) ||
								((nc == null) && (nc = comparableClassFor(k)) == null)) ||
								(dir = compareComparables(kc, nc, k, pk, n, pn)) == 0)

							dir = tieBreakOrder(k, pk, n, pn);

						TreeNode<K, N, V> xp = p;
						if ((p = (dir <= 0) ? p.left : p.right) == null) {
							x.parent = xp;
							if (dir <= 0)
								xp.left = x;
							else
								xp.right = x;
							root = balanceInsertion(root, x);
							break;
						}
					}
				}
			}
			moveRootToFront(tab, root);
		}

		/**
		 * Returns a list of non-TreeNodes replacing those linked from
		 * this node.
		 */
		final Node<K, N, V> untreeify(CoWHashMap<K, N, V> map) {
			Node<K, N, V> hd = null, tl = null;
			for (Node<K, N, V> q = this; q != null; q = q.next) {
				Node<K, N, V> p = map.replacementNode(q, null);
				if (tl == null)
					hd = p;
				else
					tl.next = p;
				tl = p;
			}
			return hd;
		}

		/**
		 * Tree version of putVal.
		 */
		final TreeNode<K, N, V> putTreeVal(CoWHashMap<K, N, V> map, Node<K, N, V>[] tab,
		                                   int h, K k, N n, V v) {
			Class<?> kc = null;
			Class<?> nc = null;
			boolean searched = false;
			TreeNode<K, N, V> root = (parent != null) ? root() : this;
			for (TreeNode<K, N, V> p = root; ; ) {
				int dir, ph;
				K pk;
				N pn = p.namespace;
				if ((ph = p.hash) > h)
					dir = -1;
				else if (ph < h)
					dir = 1;
				else if (((pk = p.key) == k || (k != null && k.equals(pk))) && (pn == n || (n != null && n.equals(pn))))
					return p;
				else if (((kc == null &&
						(kc = comparableClassFor(k)) == null) ||
						((nc == null) && (nc = comparableClassFor(k)) == null)) ||
						(dir = compareComparables(kc, nc, k, pk, n, pn)) == 0) {
					if (!searched) {
						TreeNode<K, N, V> q, ch;
						searched = true;
						if (((ch = p.left) != null &&
								(q = ch.find(h, k, n, kc, nc)) != null) ||
								((ch = p.right) != null &&
										(q = ch.find(h, k, n, kc, nc)) != null))
							return q;
					}
					dir = tieBreakOrder(k, pk, n, pn);
				}

				TreeNode<K, N, V> xp = p;
				if ((p = (dir <= 0) ? p.left : p.right) == null) {
					Node<K, N, V> xpn = xp.next;
					TreeNode<K, N, V> x = map.newTreeNode(h, k, n, v, xpn);
					if (dir <= 0)
						xp.left = x;
					else
						xp.right = x;
					xp.next = x;
					x.parent = x.prev = xp;
					if (xpn != null)
						((TreeNode<K, N, V>) xpn).prev = x;
					moveRootToFront(tab, balanceInsertion(root, x));
					return null;
				}
			}
		}

		/**
		 * Removes the given node, that must be present before this call.
		 * This is messier than typical red-black deletion code because we
		 * cannot swap the contents of an interior node with a leaf
		 * successor that is pinned by "next" pointers that are accessible
		 * independently during traversal. So instead we swap the tree
		 * linkages. If the current tree appears to have too few nodes,
		 * the bin is converted back to a plain bin. (The test triggers
		 * somewhere between 2 and 6 nodes, depending on tree structure).
		 */
		final void removeTreeNode(CoWHashMap<K, N, V> map, Node<K, N, V>[] tab,
		                          boolean movable) {
			int n;
			if (tab == null || (n = tab.length) == 0)
				return;
			int index = (n - 1) & hash;
			TreeNode<K, N, V> first = (TreeNode<K, N, V>) tab[index], root = first, rl;
			TreeNode<K, N, V> succ = (TreeNode<K, N, V>) next, pred = prev;
			if (pred == null)
				tab[index] = first = succ;
			else
				pred.next = succ;
			if (succ != null)
				succ.prev = pred;
			if (first == null)
				return;
			if (root.parent != null)
				root = root.root();
			if (root == null || root.right == null ||
					(rl = root.left) == null || rl.left == null) {
				tab[index] = first.untreeify(map);  // too small
				return;
			}
			TreeNode<K, N, V> p = this, pl = left, pr = right, replacement;
			if (pl != null && pr != null) {
				TreeNode<K, N, V> s = pr, sl;
				while ((sl = s.left) != null) // find successor
					s = sl;
				boolean c = s.red;
				s.red = p.red;
				p.red = c; // swap colors
				TreeNode<K, N, V> sr = s.right;
				TreeNode<K, N, V> pp = p.parent;
				if (s == pr) { // p was s's direct parent
					p.parent = s;
					s.right = p;
				} else {
					TreeNode<K, N, V> sp = s.parent;
					if ((p.parent = sp) != null) {
						if (s == sp.left)
							sp.left = p;
						else
							sp.right = p;
					}
					if ((s.right = pr) != null)
						pr.parent = s;
				}
				p.left = null;
				if ((p.right = sr) != null)
					sr.parent = p;
				if ((s.left = pl) != null)
					pl.parent = s;
				if ((s.parent = pp) == null)
					root = s;
				else if (p == pp.left)
					pp.left = s;
				else
					pp.right = s;
				if (sr != null)
					replacement = sr;
				else
					replacement = p;
			} else if (pl != null)
				replacement = pl;
			else if (pr != null)
				replacement = pr;
			else
				replacement = p;
			if (replacement != p) {
				TreeNode<K, N, V> pp = replacement.parent = p.parent;
				if (pp == null)
					root = replacement;
				else if (p == pp.left)
					pp.left = replacement;
				else
					pp.right = replacement;
				p.left = p.right = p.parent = null;
			}

			TreeNode<K, N, V> r = p.red ? root : balanceDeletion(root, replacement);

			if (replacement == p) {  // detach
				TreeNode<K, N, V> pp = p.parent;
				p.parent = null;
				if (pp != null) {
					if (p == pp.left)
						pp.left = null;
					else if (p == pp.right)
						pp.right = null;
				}
			}
			if (movable)
				moveRootToFront(tab, r);
		}

		/**
		 * Splits nodes in a tree bin into lower and upper tree bins,
		 * or untreeifies if now too small. Called only from resize;
		 * see above discussion about split bits and indices.
		 *
		 * @param map   the map
		 * @param tab   the table for recording bin heads
		 * @param index the index of the table being split
		 * @param bit   the bit of hash to split on
		 */
		final void split(CoWHashMap<K, N, V> map, Node<K, N, V>[] tab, int index, int bit) {
			TreeNode<K, N, V> b = this;
			// Relink into lo and hi lists, preserving order
			TreeNode<K, N, V> loHead = null, loTail = null;
			TreeNode<K, N, V> hiHead = null, hiTail = null;
			int lc = 0, hc = 0;
			for (TreeNode<K, N, V> e = b, next; e != null; e = next) {
				next = (TreeNode<K, N, V>) e.next;
				e.next = null;
				if ((e.hash & bit) == 0) {
					if ((e.prev = loTail) == null)
						loHead = e;
					else
						loTail.next = e;
					loTail = e;
					++lc;
				} else {
					if ((e.prev = hiTail) == null)
						hiHead = e;
					else
						hiTail.next = e;
					hiTail = e;
					++hc;
				}
			}

			if (loHead != null) {
				if (lc <= UNTREEIFY_THRESHOLD)
					tab[index] = loHead.untreeify(map);
				else {
					tab[index] = loHead;
					if (hiHead != null) // (else is already treeified)
						loHead.treeify(tab);
				}
			}
			if (hiHead != null) {
				if (hc <= UNTREEIFY_THRESHOLD)
					tab[index + bit] = hiHead.untreeify(map);
				else {
					tab[index + bit] = hiHead;
					if (loHead != null)
						hiHead.treeify(tab);
				}
			}
		}

        /* ------------------------------------------------------------ */
		// Red-black tree methods, all adapted from CLR

		static <K, N, V> TreeNode<K, N, V> rotateLeft(TreeNode<K, N, V> root,
		                                              TreeNode<K, N, V> p) {
			TreeNode<K, N, V> r, pp, rl;
			if (p != null && (r = p.right) != null) {
				if ((rl = p.right = r.left) != null)
					rl.parent = p;
				if ((pp = r.parent = p.parent) == null)
					(root = r).red = false;
				else if (pp.left == p)
					pp.left = r;
				else
					pp.right = r;
				r.left = p;
				p.parent = r;
			}
			return root;
		}

		static <K, N, V> TreeNode<K, N, V> rotateRight(TreeNode<K, N, V> root,
		                                               TreeNode<K, N, V> p) {
			TreeNode<K, N, V> l, pp, lr;
			if (p != null && (l = p.left) != null) {
				if ((lr = p.left = l.right) != null)
					lr.parent = p;
				if ((pp = l.parent = p.parent) == null)
					(root = l).red = false;
				else if (pp.right == p)
					pp.right = l;
				else
					pp.left = l;
				l.right = p;
				p.parent = l;
			}
			return root;
		}

		static <K, N, V> TreeNode<K, N, V> balanceInsertion(TreeNode<K, N, V> root,
		                                                    TreeNode<K, N, V> x) {
			x.red = true;
			for (TreeNode<K, N, V> xp, xpp, xppl, xppr; ; ) {
				if ((xp = x.parent) == null) {
					x.red = false;
					return x;
				} else if (!xp.red || (xpp = xp.parent) == null)
					return root;
				if (xp == (xppl = xpp.left)) {
					if ((xppr = xpp.right) != null && xppr.red) {
						xppr.red = false;
						xp.red = false;
						xpp.red = true;
						x = xpp;
					} else {
						if (x == xp.right) {
							root = rotateLeft(root, x = xp);
							xpp = (xp = x.parent) == null ? null : xp.parent;
						}
						if (xp != null) {
							xp.red = false;
							if (xpp != null) {
								xpp.red = true;
								root = rotateRight(root, xpp);
							}
						}
					}
				} else {
					if (xppl != null && xppl.red) {
						xppl.red = false;
						xp.red = false;
						xpp.red = true;
						x = xpp;
					} else {
						if (x == xp.left) {
							root = rotateRight(root, x = xp);
							xpp = (xp = x.parent) == null ? null : xp.parent;
						}
						if (xp != null) {
							xp.red = false;
							if (xpp != null) {
								xpp.red = true;
								root = rotateLeft(root, xpp);
							}
						}
					}
				}
			}
		}

		static <K, N, V> TreeNode<K, N, V> balanceDeletion(TreeNode<K, N, V> root,
		                                                   TreeNode<K, N, V> x) {
			for (TreeNode<K, N, V> xp, xpl, xpr; ; ) {
				if (x == null || x == root)
					return root;
				else if ((xp = x.parent) == null) {
					x.red = false;
					return x;
				} else if (x.red) {
					x.red = false;
					return root;
				} else if ((xpl = xp.left) == x) {
					if ((xpr = xp.right) != null && xpr.red) {
						xpr.red = false;
						xp.red = true;
						root = rotateLeft(root, xp);
						xpr = (xp = x.parent) == null ? null : xp.right;
					}
					if (xpr == null)
						x = xp;
					else {
						TreeNode<K, N, V> sl = xpr.left, sr = xpr.right;
						if ((sr == null || !sr.red) &&
								(sl == null || !sl.red)) {
							xpr.red = true;
							x = xp;
						} else {
							if (sr == null || !sr.red) {
								if (sl != null)
									sl.red = false;
								xpr.red = true;
								root = rotateRight(root, xpr);
								xpr = (xp = x.parent) == null ?
										null : xp.right;
							}
							if (xpr != null) {
								xpr.red = (xp == null) ? false : xp.red;
								if ((sr = xpr.right) != null)
									sr.red = false;
							}
							if (xp != null) {
								xp.red = false;
								root = rotateLeft(root, xp);
							}
							x = root;
						}
					}
				} else { // symmetric
					if (xpl != null && xpl.red) {
						xpl.red = false;
						xp.red = true;
						root = rotateRight(root, xp);
						xpl = (xp = x.parent) == null ? null : xp.left;
					}
					if (xpl == null)
						x = xp;
					else {
						TreeNode<K, N, V> sl = xpl.left, sr = xpl.right;
						if ((sl == null || !sl.red) &&
								(sr == null || !sr.red)) {
							xpl.red = true;
							x = xp;
						} else {
							if (sl == null || !sl.red) {
								if (sr != null)
									sr.red = false;
								xpl.red = true;
								root = rotateLeft(root, xpl);
								xpl = (xp = x.parent) == null ?
										null : xp.left;
							}
							if (xpl != null) {
								xpl.red = (xp == null) ? false : xp.red;
								if ((sl = xpl.left) != null)
									sl.red = false;
							}
							if (xp != null) {
								xp.red = false;
								root = rotateRight(root, xp);
							}
							x = root;
						}
					}
				}
			}
		}

		/**
		 * Recursive invariant check
		 */
		static <K, N, V> boolean checkInvariants(TreeNode<K, N, V> t) {
			TreeNode<K, N, V> tp = t.parent, tl = t.left, tr = t.right,
					tb = t.prev, tn = (TreeNode<K, N, V>) t.next;
			if (tb != null && tb.next != t)
				return false;
			if (tn != null && tn.prev != t)
				return false;
			if (tp != null && t != tp.left && t != tp.right)
				return false;
			if (tl != null && (tl.parent != t || tl.hash > t.hash))
				return false;
			if (tr != null && (tr.parent != t || tr.hash < t.hash))
				return false;
			if (t.red && tl != null && tl.red && tr != null && tr.red)
				return false;
			if (tl != null && !checkInvariants(tl))
				return false;
			if (tr != null && !checkInvariants(tr))
				return false;
			return true;
		}
	}
}