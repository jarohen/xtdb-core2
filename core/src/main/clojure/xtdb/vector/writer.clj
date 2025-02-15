(ns xtdb.vector.writer
  (:require [clojure.set :as set]
            [xtdb.error :as err]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.vector :as vec]
            [xtdb.vector.indirect :as iv])
  (:import (java.lang AutoCloseable)
           (java.util HashMap LinkedHashMap Map)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.util AutoCloseables)
           (org.apache.arrow.vector ExtensionTypeVector NullVector ValueVector)
           (org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector ListVector StructVector)
           (org.apache.arrow.vector.types.pojo ArrowType$List ArrowType$Struct ArrowType$Union Field FieldType)
           (xtdb.vector IDenseUnionWriter IExtensionWriter IIndirectRelation IIndirectVector IListWriter IRelationWriter IRowCopier IStructWriter IVectorWriter IWriterPosition)
           xtdb.vector.extensions.SetType))

(deftype DuvChildWriter [^IDenseUnionWriter parent-writer,
                         ^byte type-id
                         ^IVectorWriter inner-writer]
  IVectorWriter
  (getVector [_] (.getVector inner-writer))
  (getPosition [_] (.getPosition inner-writer))

  (asStruct [duv-child-writer]
    (let [^IStructWriter inner-writer (cast IStructWriter inner-writer)] ; cast to throw CCE early
      (reify
        IStructWriter
        (getVector [_] (.getVector inner-writer))
        (getPosition [_] (.getPosition inner-writer))
        (writerForName [_ col-name] (.writerForName inner-writer col-name))
        (startValue [_] (.startValue duv-child-writer))
        (endValue [_] (.endValue duv-child-writer)))))

  (asList [duv-child-writer]
    (let [^IListWriter inner-writer (cast IListWriter inner-writer)] ; cast to throw CCE early
      (reify
        IListWriter
        (getVector [_] (.getVector inner-writer))
        (getPosition [_] (.getPosition inner-writer))
        (startValue [_] (.startValue duv-child-writer))
        (endValue [_] (.endValue duv-child-writer))
        (getDataWriter [_] (.getDataWriter inner-writer)))))

  (asExtension [duv-child-writer]
    (let [^IExtensionWriter inner-writer (cast IExtensionWriter inner-writer)] ; cast to throw CCE early
      (reify
        IExtensionWriter
        (getVector [_] (.getVector inner-writer))
        (getPosition [_] (.getPosition inner-writer))
        (startValue [_] (.startValue duv-child-writer))
        (endValue [_] (.endValue duv-child-writer))
        (getUnderlyingWriter [_] (.getUnderlyingWriter inner-writer)))))

  (startValue [this]
    (let [parent-duv (.getVector parent-writer)
          parent-pos (.getPosition parent-writer)
          pos (.getPosition this)]
      (.setTypeId parent-duv parent-pos type-id)
      (.setOffset parent-duv parent-pos pos)

      ;; TODO (#252) do we still need this here?
      (.setValueCount parent-duv (inc parent-pos))

      (.startValue inner-writer)

      pos))

  (endValue [_] (.endValue inner-writer))
  (clear [_] (.clear inner-writer))

  (rowCopier [this-writer src-col]
    (let [inner-copier (.rowCopier inner-writer src-col)]
      (reify IRowCopier
        (copyRow [_this src-idx]
          (let [pos (.startValue this-writer)]
            (.copyRow inner-copier src-idx)
            (.endValue this-writer)
            pos))))))

(defn- duv->duv-copier ^xtdb.vector.IRowCopier [^DenseUnionVector src-vec, ^IDenseUnionWriter dest-col]
  (let [src-field (.getField src-vec)
        src-type (.getType src-field)
        type-ids (.getTypeIds ^ArrowType$Union src-type)
        child-fields (.getChildren src-field)
        child-count (count child-fields)
        copier-mapping (object-array child-count)]

    (dotimes [n child-count]
      (let [src-type-id (or (when type-ids (aget type-ids n))
                            n)
            col-type (types/field->col-type (.get child-fields n))]
        (aset copier-mapping src-type-id (.rowCopier (.writerForType dest-col col-type)
                                                     (.getVectorByType src-vec src-type-id)))))

    (reify IRowCopier
      (copyRow [_ src-idx]
        (let [type-id (.getTypeId src-vec src-idx)]
          (assert (not (neg? type-id)))
          (-> ^IRowCopier (aget copier-mapping type-id)
              (.copyRow (.getOffset src-vec src-idx))))))))

(defn- vec->duv-copier ^xtdb.vector.IRowCopier [^ValueVector src-vec, ^IDenseUnionWriter dest-col]
  (let [field (.getField src-vec)
        col-type (types/field->col-type field)]
    (zmatch col-type
      [:union inner-types]
      (let [without-null (disj inner-types :null)]
        (assert (= 1 (count without-null)))
        (let [nn-col-type (first without-null)
              non-null-copier (-> (.writerForType dest-col nn-col-type)
                                  (.rowCopier src-vec))
              null-copier (-> (.writerForType dest-col :null)
                              (.rowCopier src-vec))]
          (reify IRowCopier
            (copyRow [_ src-idx]
              (if (.isNull src-vec src-idx)
                (.copyRow null-copier src-idx)
                (.copyRow non-null-copier src-idx))))))

      (-> (.writerForType dest-col col-type)
          (.rowCopier src-vec)))))

(declare ^xtdb.vector.IVectorWriter vec->writer)

(defn- duv-type-id ^java.lang.Byte [^DenseUnionVector duv, col-type]
  (let [field (.getField duv)
        type-ids (.getTypeIds ^ArrowType$Union (.getType field))
        duv-leg-key (types/col-type->duv-leg-key col-type)]
    (-> (keep-indexed (fn [idx ^Field sub-field]
                        (when (= duv-leg-key (-> (types/field->col-type sub-field)
                                                 (types/col-type->duv-leg-key)))
                          (aget type-ids idx)))
                      (.getChildren field))
        (first))))

(deftype DuvWriter [^DenseUnionVector dest-duv,
                    ^"[Lxtdb.vector.IVectorWriter;" writers-by-type-id
                    ^Map writers-by-type
                    ^:unsynchronized-mutable ^int pos]
  IVectorWriter
  (getVector [_] dest-duv)
  (getPosition [_] pos)

  (startValue [_]
    ;; allocates memory in the type-id/offset buffers even if the DUV value is null
    (.setTypeId dest-duv pos -1)
    (.setOffset dest-duv pos 0)
    pos)

  (endValue [this]
    (when (neg? (.getTypeId dest-duv pos))
      (doto (.writerForType this :absent)
        (.startValue)
        (.endValue)))

    (set! (.pos this) (inc pos)))

  (clear [this]
    (.clear dest-duv)
    (doseq [^IVectorWriter writer writers-by-type-id
            :when writer]
      (.clear writer))
    (set! (.pos this) 0))

  (rowCopier [this-writer src-vec]
    (let [inner-copier (if (instance? DenseUnionVector src-vec)
                         (duv->duv-copier src-vec this-writer)
                         (vec->duv-copier src-vec this-writer))]
      (reify IRowCopier
        (copyRow [_ src-idx] (.copyRow inner-copier src-idx)))))

  IDenseUnionWriter
  (writerForTypeId [this type-id]
    (or (aget writers-by-type-id type-id)
        (let [^ValueVector
              inner-vec (or (.getVectorByType dest-duv type-id)
                            (throw (err/illegal-arg ::invalid-duv-type-id)))
              inner-writer (vec->writer inner-vec)
              writer (DuvChildWriter. this type-id inner-writer)]
          (aset writers-by-type-id type-id writer)
          writer)))

  (registerNewType [_ field]
    (let [type-id (.registerNewTypeId dest-duv field)]
      (.addVector dest-duv type-id (.createVector field (.getAllocator dest-duv)))
      type-id))

  (writerForType [this col-type]
    (.computeIfAbsent writers-by-type (types/col-type->duv-leg-key col-type)
                      (reify Function
                        (apply [_ _]
                          (let [field-name (types/col-type->field-name col-type)

                                ^Field field (case (types/col-type-head col-type)
                                               :list
                                               (types/->field field-name ArrowType$List/INSTANCE false (types/->field "$data$" types/dense-union-type false))

                                               :set
                                               (types/->field field-name SetType/INSTANCE false (types/->field "$data$" types/dense-union-type false))

                                               :struct
                                               (types/->field field-name ArrowType$Struct/INSTANCE false)

                                               (types/col-type->field field-name col-type))

                                type-id (or (duv-type-id dest-duv col-type)
                                            (.registerNewType this field))]

                            (.writerForTypeId this type-id)))))))

(defn- populate-with-absents [^IDenseUnionWriter w, ^long pos]
  (when (pos? pos)
    (let [absent-writer (.writerForType w :absent)]
      (dotimes [_ pos]
        (.startValue w)
        (.startValue absent-writer)
        (.endValue absent-writer)
        (.endValue w)))))

(deftype StructWriter [^StructVector dest-vec, ^Map writers,
                       ^:unsynchronized-mutable ^int pos]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)

  (startValue [_]
    (.setIndexDefined dest-vec pos)
    (doseq [^IVectorWriter writer (vals writers)]
      (.startValue writer))
    pos)

  (endValue [this]
    (doseq [^IVectorWriter writer (vals writers)]
      (.endValue writer))
    (set! (.pos this) (inc pos)))

  (clear [this]
    (.clear dest-vec)
    (doseq [^IVectorWriter writer (vals writers)]
      (.clear writer))
    (set! (.pos this) 0))

  (rowCopier [struct-writer src-vec]
    (let [^StructVector src-vec (cast StructVector src-vec)
          copiers (vec (for [^ValueVector child-vec (.getChildrenFromFields src-vec)]
                         (.rowCopier (.writerForName struct-writer (.getName child-vec))
                                     child-vec)))]
      (reify IRowCopier
        (copyRow [_ src-idx]
          (doseq [^IRowCopier copier copiers]
            (.copyRow copier src-idx))
          pos))))

  IStructWriter
  (writerForName [_ col-name]
    (.computeIfAbsent writers col-name
                      (reify Function
                        (apply [_ col-name]
                          (or (some-> (.getChild dest-vec col-name)
                                      (vec->writer pos))

                              (doto (vec->writer (.addOrGet dest-vec col-name
                                                            (FieldType/notNullable types/dense-union-type)
                                                            DenseUnionVector))
                                (populate-with-absents pos))))))))

(deftype ListWriter [^ListVector dest-vec
                     ^IVectorWriter data-writer
                     ^:unsynchronized-mutable ^int pos
                     ^:unsynchronized-mutable ^int data-start-pos]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)

  (startValue [this]
    (set! (.data-start-pos this) (.startNewValue dest-vec pos))
    pos)

  (endValue [this]
    (.endValue dest-vec pos (- (.getPosition data-writer) data-start-pos))
    (set! (.pos this) (inc pos)))

  (clear [this]
    (.clear data-writer)
    (.clear dest-vec)
    (set! (.pos this) 0))

  (rowCopier [this-list-writer src-vec]
    (let [^ListVector src-vec (cast ListVector src-vec)
          src-data-vec (.getDataVector src-vec)
          inner-copier (.rowCopier data-writer src-data-vec)]
      (reify IRowCopier
        (copyRow [_ src-idx]
          (let [pos (.pos this-list-writer)]
            (if (.isNull src-vec src-idx)
              (.setNull dest-vec pos)
              (do
                (.setNotNull dest-vec pos)
                (let [start-idx (.getElementStartIndex src-vec src-idx)]
                  (dotimes [el-idx (- (.getElementEndIndex src-vec src-idx) start-idx)]
                    (.startValue data-writer)
                    (.copyRow inner-copier (+ start-idx el-idx))
                    (.endValue data-writer))))))
          pos))))

  IListWriter
  (getDataWriter [_] data-writer))

(deftype FixedSizeListWriter [^FixedSizeListVector dest-vec
                              ^IVectorWriter data-writer
                              ^:unsynchronized-mutable ^int pos]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)

  (startValue [_]
    (.setNotNull dest-vec pos)
    pos)

  (endValue [this] (set! (.pos this) (inc pos)))

  (clear [this]
    (.clear data-writer)
    (.clear dest-vec)
    (set! (.pos this) 0))

  (rowCopier [this-list-writer src-vec]
    (let [^FixedSizeListVector src-vec (cast FixedSizeListVector src-vec)
          src-data-vec (.getDataVector src-vec)
          inner-copier (.rowCopier data-writer src-data-vec)]
      (reify IRowCopier
        (copyRow [_ src-idx]
          (let [pos (.pos this-list-writer)]
            (if (.isNull src-vec src-idx)
              (.setNull dest-vec pos)
              (do
                (.setNotNull dest-vec pos)
                (let [start-idx (.getElementStartIndex src-vec src-idx)]
                  (dotimes [el-idx (- (.getElementEndIndex src-vec src-idx) start-idx)]
                    (.startValue data-writer)
                    (.copyRow inner-copier (+ start-idx el-idx))
                    (.endValue data-writer))))))
          pos))))

  IListWriter
  (getDataWriter [_] data-writer))

(deftype ExtensionWriter [^ExtensionTypeVector dest-vec, ^IVectorWriter underlying-writer]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] (.getPosition underlying-writer))
  (startValue [_] (.startValue underlying-writer))
  (endValue [_] (.endValue underlying-writer))

  (clear [_]
    (.clear underlying-writer)
    (.clear dest-vec))

  (rowCopier [this-writer src-vec]
    (cond
      ;; assume it's a one-leg DUV
      (instance? DenseUnionVector src-vec) (.rowCopier this-writer (first (seq src-vec)))

      (instance? NullVector src-vec) (reify IRowCopier
                                       (copyRow [_ _src-idx]
                                         (let [pos (.getPosition this-writer)]
                                           ;; TODO (#252) do we still need this here?
                                           (.setValueCount dest-vec (inc pos))
                                           pos)))

      :else (.rowCopier underlying-writer (.getUnderlyingVector ^ExtensionTypeVector src-vec))))

  (monoWriter [_ col-type] (vec/->mono-writer dest-vec col-type))
  (polyWriter [_ col-type] (vec/->poly-writer dest-vec col-type))

  IExtensionWriter
  (getUnderlyingWriter [_] underlying-writer))

(deftype ScalarWriter [^ValueVector dest-vec,
                       ^:unsynchronized-mutable ^int pos]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)
  (startValue [_] pos)
  (endValue [this] (set! (.pos this) (inc pos)))

  (clear [this]
    (.clear dest-vec)
    (set! (.pos this) 0))

  (rowCopier [this-writer src-vec]
    (cond
      (instance? NullVector dest-vec)
      ;; `NullVector/.copyFromSafe` throws UOE
      (reify IRowCopier
        (copyRow [_ _src-idx] (.getPosition this-writer)))

      (instance? NullVector src-vec)
      (reify IRowCopier
        (copyRow [_ _src-idx]
          (let [pos (.getPosition this-writer)]
            ;; TODO (#252) do we still need this here?
            (.setValueCount dest-vec (inc pos))
            pos)))

      (instance? DenseUnionVector src-vec)
      (let [^DenseUnionVector src-vec src-vec
            copiers (object-array (for [child-vec src-vec]
                                    (.rowCopier this-writer child-vec)))]
        (reify IRowCopier
          (copyRow [_ src-idx]
            (.copyRow ^IRowCopier (aget copiers (.getTypeId src-vec src-idx))
                      (.getOffset src-vec src-idx)))))

      :else
      (reify IRowCopier
        (copyRow [_ src-idx]
          (let [pos (.getPosition this-writer)]
            (.copyFromSafe dest-vec src-idx pos src-vec)
            pos)))))

  (monoWriter [_ col-type] (vec/->mono-writer dest-vec col-type))
  (polyWriter [_ col-type] (vec/->poly-writer dest-vec col-type)))

(defn vec->writer
  (^xtdb.vector.IVectorWriter
   [^ValueVector dest-vec] (vec->writer dest-vec (.getValueCount dest-vec)))

  (^xtdb.vector.IVectorWriter
   [^ValueVector dest-vec, ^long pos]
   (cond
     (instance? DenseUnionVector dest-vec)
     ;; eugh. we have to initialise the writers map if the DUV is already populated.
     ;; easiest way to do this (that I can see) is to re-use `.writerForTypeId`.
     ;; if I'm making a dog's dinner of this, feel free to refactor.
     (let [writers-by-type (HashMap.)
           writer (DuvWriter. dest-vec
                              (make-array IVectorWriter (inc Byte/MAX_VALUE))
                              writers-by-type
                              pos)]
       (dotimes [writer-idx (count (seq dest-vec))]
         (let [inner-writer (.writerForTypeId writer writer-idx)
               inner-vec (.getVector inner-writer)
               duv-leg-key (-> (types/field->col-type (.getField inner-vec))
                               types/col-type->duv-leg-key)]
           (.put writers-by-type duv-leg-key inner-writer)))

       writer)

     (instance? StructVector dest-vec)
     (StructWriter. dest-vec (HashMap.) pos)

     (instance? ListVector dest-vec)
     (ListWriter. dest-vec (vec->writer (.getDataVector ^ListVector dest-vec)) pos 0)

     (instance? FixedSizeListVector dest-vec)
     (FixedSizeListWriter. dest-vec (vec->writer (.getDataVector ^FixedSizeListVector dest-vec)) pos)

     (instance? ExtensionTypeVector dest-vec)
     (ExtensionWriter. dest-vec (vec->writer (.getUnderlyingVector ^ExtensionTypeVector dest-vec)))

     :else
     (ScalarWriter. dest-vec pos))))

(defn ->vec-writer
  (^xtdb.vector.IVectorWriter [^BufferAllocator allocator, col-name]
   (vec->writer (-> (types/->field col-name types/dense-union-type false)
                    (.createVector allocator))))

  (^xtdb.vector.IVectorWriter [^BufferAllocator allocator, col-name, col-type]
   (vec->writer (-> (types/col-type->field col-name col-type)
                    (.createVector allocator)))))

(defn ->row-copier ^xtdb.vector.IRowCopier [^IVectorWriter vec-writer, ^IIndirectVector in-col]
  (let [in-vec (.getVector in-col)
        row-copier (.rowCopier vec-writer in-vec)]
    (reify IRowCopier
      (copyRow [_ src-idx]
        (let [pos (.startValue vec-writer)]
          (.copyRow row-copier (.getIndex in-col src-idx))
          (.endValue vec-writer)
          pos)))))

(defn ->rel-writer ^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
  (let [writers (LinkedHashMap.)
        wp (IWriterPosition/build)]
    (reify IRelationWriter
      (writerPosition [_] wp)

      (writerForName [_ col-name]
        (.computeIfAbsent writers col-name
                          (reify Function
                            (apply [_ col-name]
                              (doto (->vec-writer allocator col-name)
                                (populate-with-absents (.getPosition wp)))))))

      (writerForName [_ col-name col-type]
        (.computeIfAbsent writers col-name
                          (reify Function
                            (apply [_ col-name]
                              (let [pos (.getPosition wp)]
                                (if (pos? pos)
                                  (doto (->vec-writer allocator col-name (types/merge-col-types col-type :absent))
                                    (populate-with-absents pos))
                                  (->vec-writer allocator col-name col-type)))))))

      (rowCopier [this in-rel]
        (let [copiers (vec (concat (for [^IIndirectVector in-vec in-rel]
                                     (->row-copier (.writerForName this (.getName in-vec)) in-vec))
                                   (for [absent-col-name (set/difference (set (keys writers))
                                                                         (into #{} (map #(.getName ^IIndirectVector %)) in-rel))
                                         :let [writer (.writerForName this absent-col-name)]]
                                     (reify IRowCopier
                                       (copyRow [_ _src-idx]
                                         (let [pos (.startValue writer)]
                                           (.endValue writer)
                                           pos))))))]
          (reify IRowCopier
            (copyRow [_ src-idx]
              (let [pos (.getPositionAndIncrement wp)]
                (doseq [^IRowCopier copier copiers]
                  (.copyRow copier src-idx))
                pos)))))

      (iterator [_]
        (.iterator (.values writers)))

      AutoCloseable
      (close [_]
        (AutoCloseables/close (.values writers))))))

(defn rel-writer->reader ^xtdb.vector.IIndirectRelation [^IRelationWriter rel-writer]
  (iv/->indirect-rel (for [^IVectorWriter vec-writer rel-writer]
                       (iv/->direct-vec (.getVector vec-writer)))))

(defn append-vec [^IVectorWriter vec-writer, ^IIndirectVector in-col]
  (let [row-copier (->row-copier vec-writer in-col)
        v (.getVector vec-writer)
        init-value-count (.getValueCount v)
        src-value-count (.getValueCount in-col)]
    (dotimes [src-idx src-value-count]
      (.copyRow row-copier src-idx))
    (.setValueCount v (+ init-value-count src-value-count))))

(defn append-rel [^IRelationWriter dest-rel, ^IIndirectRelation src-rel]
  (doseq [^IIndirectVector src-col src-rel
          :let [col-type (types/field->col-type (.getField (.getVector src-col)))
                ^IVectorWriter vec-writer (.writerForName dest-rel (.getName src-col) col-type)]]
    (append-vec vec-writer src-col))

  (let [wp (.writerPosition dest-rel)]
    (.setPosition wp (+ (.getPosition wp) (.rowCount src-rel)))))

(defn write-vec! [^ValueVector v, vs]
  (.clear v)

  (let [duv? (instance? DenseUnionVector v)
        writer (vec->writer v)]
    (doseq [v vs]
      (.startValue writer)
      (if duv?
        (doto (.writerForType (.asDenseUnion writer) (types/value->col-type v))
          (.startValue)
          (->> (types/write-value! v))
          (.endValue))

        (types/write-value! v writer))

      (.endValue writer))

    (.setValueCount v (count vs))

    v))

(defn open-vec
  (^org.apache.arrow.vector.ValueVector [allocator col-name vs]
   (open-vec allocator col-name
             (->> (into #{} (map types/value->col-type) vs)
                  (apply types/merge-col-types))
             vs))

  (^org.apache.arrow.vector.ValueVector [allocator col-name col-type vs]
   (let [res (-> (types/col-type->field col-name col-type)
                 (.createVector allocator))]
     (try
       (doto res (write-vec! vs))
       (catch Throwable e
         (.close res)
         (throw e))))))

(defn open-rel ^xtdb.vector.IIndirectRelation [vecs]
  (iv/->indirect-rel (map iv/->direct-vec vecs)))

(defn open-params ^xtdb.vector.IIndirectRelation [allocator params-map]
  (open-rel (for [[k v] params-map]
              (open-vec allocator k [v]))))

(def empty-params (iv/->indirect-rel [] 1))
