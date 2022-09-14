(ns core2.operator.external-data-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util])
  (:import org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.VectorSchemaRoot
           java.net.URL))

(t/use-fixtures :once tu/with-allocator)

(def example-data
  [[{:id "foo1", :a_long 10, :a_double 54.2, :an_inst (util/->zdt #inst "2021")}
    {:id "foo2", :a_long 82, :a_double 1052.25, :an_inst (util/->zdt #inst "2021-01-04")}
    {:id "foo3", :a_long -15, :a_double -1534.23, :an_inst (util/->zdt #inst "2021-01-04T12:13")}]
   [{:id "foo4", :a_long 0, :a_double 0.0, :an_inst (util/->zdt #inst "2021-05-21T17:30")}
    {:id "foo5", :a_long 53, :a_double 10.0, :an_inst (util/->zdt #inst "2022")}]])

(t/deftest test-csv-cursor
  (t/is (= {:col-types {"id" :utf8, "a_long" :i64, "a_double" :f64, "an_inst" [:timestamp-tz :micro "UTC"]}
            :res example-data}
           (tu/query-ra [:csv (-> (io/resource "core2/operator/csv-cursor-test.csv")
                                  .toURI
                                  util/->path)
                         '{id :utf8
                           a_long :i64
                           a_double :f64
                           an_inst :timestamp}
                         {:batch-size 3}]
                        {:preserve-blocks? true
                         :with-col-types? true}))))

(def ^:private arrow-stream-url
  (io/resource "core2/operator/arrow-cursor-test.arrows"))

(def ^:private arrow-file-url
  (io/resource "core2/operator/arrow-cursor-test.arrow"))

(t/deftest test-arrow-cursor
  (let [expected {:col-types '{id :utf8, a_long :i64, a_double :f64, an_inst [:timestamp-tz :micro "UTC"]}
                  :res example-data}]
    (t/is (= expected (tu/query-ra [:arrow arrow-file-url]
                                   {:preserve-blocks? true, :with-col-types? true})))
    (t/is (= expected (tu/query-ra [:arrow arrow-stream-url]
                                   {:preserve-blocks? true, :with-col-types? true})))))

(comment
  (doseq [[^URL url arrow-mode] [[arrow-stream-url :stream]
                                 [arrow-file-url :file]]]
    (let [arrow-path (-> (.toURI url) (util/->path))]
      (with-open [al (RootAllocator.)
                  root (VectorSchemaRoot/create (Schema. [(types/col-type->field "id" :utf8)
                                                          (types/col-type->field "a_long" :i64)
                                                          (types/col-type->field "a_double" :f64)
                                                          (types/col-type->field "an_inst" [:timestamp-tz :micro "UTC"])])
                                                al)]
        (doto (util/build-arrow-ipc-byte-buffer root arrow-mode
                (fn [write-batch!]
                  (doseq [block example-data]
                    (tu/populate-root root block)
                    (write-batch!))))
          (util/write-buffer-to-path arrow-path))))))
