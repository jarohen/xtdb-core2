package core2.vector;

import org.apache.arrow.vector.ValueVector;

@SuppressWarnings("try")
public interface IVectorWriter2 extends AutoCloseable {
    ValueVector getVector();

    IWriterPosition writerPosition();

    IRowCopier2 rowCopier(ValueVector vector);
}
