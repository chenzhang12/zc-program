package zc.test;

import java.util.Map;

public class TestMapEntry {
	
	static class Pair<K, V> implements Map.Entry<K, V> {
		
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

	public static void main(String[] args) {
		Map.Entry<Long, Long> entry = new Pair<>(1l, 2l);
		System.out.println(entry.getKey()+", "+entry.getValue());
	}

}
