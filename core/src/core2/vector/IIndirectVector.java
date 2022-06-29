package core2.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

import java.util.List;

public interface IIndirectVector<V extends ValueVector> extends AutoCloseable {

    boolean isPresent(int idx);

    V getVector();

    int getIndex(int idx);

    String getName();

    int getValueCount();

    IIndirectVector<V> withName(String colName);

    @SuppressWarnings("unchecked")
    default IIndirectVector<V> copy(BufferAllocator allocator) {
        var res = (V) getVector().getField().createVector(allocator);
        try {
            return copyTo(res);
        } catch (Throwable e) {
            res.close();
            throw e;
        }
    }

    IIndirectVector<V> copyTo(V vector);

    IIndirectVector<V> select(int[] idxs);

    /**
     * looking to replace {@link #rowCopier(IVectorWriter)} with {@link #rowCopier2(IVectorWriter2)} - I'll deprecate this one later
     */
    IRowCopier rowCopier(IVectorWriter<? super V> writer);

    IStructReader structReader();
    IListReader listReader();

    IRowCopier2 rowCopier2(IVectorWriter2 writer);

    IMonoVectorReader monoReader();
    IPolyVectorReader polyReader(List<Object> orderedColTypes);

    @Override
    default void close() {
        getVector().close();
    }
}
