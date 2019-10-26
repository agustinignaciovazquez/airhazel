package ar.edu.itba.pod.api.util;

import ar.edu.itba.pod.api.model.enums.Field;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.util.Collection;

public interface Importer<T> {

    void importToIList(IList<T> iList, Collection<T> collection);

    void importToIMap(IMap<String, T> iMap, Collection<T> collection, Field field);

    void importToMultiMap(MultiMap<String, T> multiMap, Collection<T> collection, Field field);
}