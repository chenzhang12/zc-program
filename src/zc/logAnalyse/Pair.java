package zc.logAnalyse;

import java.util.Map;

class Pair<K, V> implements Map.Entry<K, V> {
	
	K key;
	V value;
	
	public Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public V getValue() {
		return value;
	}

	@Override
	public V setValue(V value) {
		this.value = value;
		return value;
	}
	
}
