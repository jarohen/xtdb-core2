(ns xtdb.operator.unwind
  (:require [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.util :as util]
            [xtdb.vector.indirect :as iv]
            [xtdb.vector.writer :as vw]
            [xtdb.types :as types])
  (:import (xtdb ICursor)
           (xtdb.vector IIndirectRelation IIndirectVector IVectorWriter)
           (java.util LinkedList)
           (java.util.function Consumer IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector IntVector)
           (org.apache.arrow.vector.complex
             BaseListVector
             DenseUnionVector
             FixedSizeListVector
             ListVector)))

(s/def ::ordinality-column ::lp/column)

(defmethod lp/ra-expr :unwind [_]
  (s/cat :op #{:ω :unwind}
         :columns (s/map-of ::lp/column ::lp/column, :conform-keys true, :count 1)
         :opts (s/? (s/keys :opt-un [::ordinality-column]))
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn- get-data-vector ^org.apache.arrow.vector.ValueVector [^BaseListVector v]
  (if (instance? FixedSizeListVector v)
    (.getDataVector ^FixedSizeListVector v)
    (.getDataVector ^ListVector v)))

(defn- unwind-list-element ^long [^BaseListVector in-vec ^IVectorWriter out-writer ^long idx]
  (let [data-vector (get-data-vector in-vec)
        row-copier (.rowCopier out-writer data-vector)
        element-start-idx (.getElementStartIndex in-vec idx)
        elements-at-idx (- (.getElementEndIndex in-vec idx) element-start-idx)]
    (dotimes [n elements-at-idx]
      (.startValue out-writer)
      (.copyRow row-copier (+ element-start-idx n))
      (.endValue out-writer))
    elements-at-idx))

(deftype UnwindCursor [^BufferAllocator allocator
                       ^ICursor in-cursor
                       ^String from-column-name
                       ^String to-column-name
                       ^String ordinality-column]
  ICursor
  (tryAdvance [_this c]
    (let [advanced? (boolean-array 1)]
      (while (and (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IIndirectRelation in-rel in-rel
                                         out-cols (LinkedList.)
                                         from-col (.vectorForName in-rel from-column-name)
                                         idxs (IntStream/builder)
                                         in-vec (.getVector from-col)

                                         ordinal-vec (when ordinality-column
                                                       (IntVector. ordinality-column allocator))]

                                     (try
                                       (let [^IntConsumer
                                             add-ordinal (if ordinal-vec
                                                           (let [w (vw/vec->writer ordinal-vec)]
                                                             (.add out-cols (iv/->direct-vec ordinal-vec))

                                                             (reify IntConsumer
                                                               (accept [_ ordinal]
                                                                 (let [pos (.startValue w)]
                                                                   (.setSafe ordinal-vec pos ordinal)
                                                                   (.endValue w)
                                                                   (.setValueCount ordinal-vec (inc pos))))))

                                                           (reify IntConsumer
                                                             (accept [_ _ordinal])))]

                                         (with-open [out-vec (DenseUnionVector/empty to-column-name allocator)]
                                           (let [out-writer (vw/vec->writer out-vec)]
                                             (cond
                                               (instance? DenseUnionVector in-vec)
                                               (let [^DenseUnionVector in-vec in-vec]
                                                 (dotimes [n (.getValueCount from-col)]
                                                   (let [idx (.getIndex from-col n)
                                                         inner-vec (.getVectorByType in-vec (.getTypeId in-vec idx))]
                                                     (when (instance? BaseListVector inner-vec)
                                                       (dotimes [m (unwind-list-element inner-vec out-writer (.getOffset in-vec idx))]
                                                         (.add idxs n)
                                                         (.accept add-ordinal (inc m)))))))

                                               (instance? BaseListVector in-vec)
                                               (dotimes [n (.getValueCount from-col)]
                                                 (dotimes [m (unwind-list-element in-vec out-writer (.getIndex from-col n))]
                                                   (.add idxs n)
                                                   (.accept add-ordinal (inc m))))))

                                           (let [idxs (.toArray (.build idxs))]
                                             (when (pos? (alength idxs))
                                               (doseq [^IIndirectVector in-col in-rel]
                                                 (when (= from-column-name (.getName in-col))
                                                   (.add out-cols (iv/->direct-vec out-vec)))

                                                 (.add out-cols (.select in-col idxs)))

                                               (.accept c (iv/->indirect-rel out-cols))
                                               (aset advanced? 0 true)))))
                                       (finally
                                         (util/try-close ordinal-vec)))))))

                  (not (aget advanced? 0))))
      (aget advanced? 0)))

  (close [_]
    (.close in-cursor)))

(defmethod lp/emit-expr :unwind [{:keys [columns relation], {:keys [ordinality-column]} :opts}, op-args]
  (let [[to-col from-col] (first columns)]
    (lp/unary-expr (lp/emit-expr relation op-args)
                   (fn [col-types]
                     (let [unwind-col-type (->> (get col-types from-col)
                                                types/flatten-union-types
                                                (keep (fn [col-type]
                                                        (zmatch col-type
                                                                [:list inner-type] inner-type
                                                                [:fixed-size-list _list-size inner-type] inner-type)))
                                                (apply types/merge-col-types))]
                       {:col-types (-> col-types
                                       (assoc to-col unwind-col-type)
                                       (cond-> ordinality-column (assoc ordinality-column :i32)))
                        :->cursor (fn [{:keys [allocator]} in-cursor]
                                    (UnwindCursor. allocator in-cursor
                                                   (str from-col) (str to-col)
                                                   (some-> ordinality-column name)))})))))
