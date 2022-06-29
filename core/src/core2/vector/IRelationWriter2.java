package core2.vector;

import clojure.lang.Symbol;

import java.util.Map;

@SuppressWarnings("try")
public interface IRelationWriter2 extends AutoCloseable, Iterable<IVectorWriter2> {
    IWriterPosition writerPosition();

    IVectorWriter2 writerFor(Symbol colName);

    IRowCopier2 rowCopier(IIndirectRelation relation);
}
