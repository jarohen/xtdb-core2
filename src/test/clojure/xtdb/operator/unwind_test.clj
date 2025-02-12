(ns xtdb.operator.unwind-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-unwind
  (let [in-vals [[{:a 1 :b [1 2]} {:a 2 :b [3 4 5]}]
                 [{:a 3 :b []}]
                 [{:a 4 :b [6 7 8]} {:a 5 :b []}]]]

    (t/is (= {:col-types '{a :i64, b [:list :i64], b* :i64}
              :res [[{:a 1, :b [1 2], :b* 1}
                     {:a 1, :b [1 2], :b* 2}
                     {:a 2, :b [3 4 5], :b* 3}
                     {:a 2, :b [3 4 5], :b* 4}
                     {:a 2, :b [3 4 5], :b* 5}]
                    [{:a 4, :b [6 7 8], :b* 6}
                     {:a 4, :b [6 7 8], :b* 7}
                     {:a 4, :b [6 7 8], :b* 8}]]}
             (tu/query-ra [:unwind '{b* b}
                           [::tu/blocks '{a :i64, b [:list :i64]} in-vals]]
                          {:preserve-blocks? true
                           :with-col-types? true})))

    (t/is (= {:col-types '{a :i64, b [:list :i64], b* :i64, $ordinal :i32}
              :res [[{:a 1, :b [1 2], :b* 1, :$ordinal 1}
                     {:a 1, :b [1 2], :b* 2, :$ordinal 2}
                     {:a 2, :b [3 4 5], :b* 3, :$ordinal 1}
                     {:a 2, :b [3 4 5], :b* 4, :$ordinal 2}
                     {:a 2, :b [3 4 5], :b* 5, :$ordinal 3}]
                    [{:a 4, :b [6 7 8], :b* 6, :$ordinal 1}
                     {:a 4, :b [6 7 8], :b* 7, :$ordinal 2}
                     {:a 4, :b [6 7 8], :b* 8, :$ordinal 3}]]}
             (tu/query-ra [:unwind '{b* b} '{:ordinality-column $ordinal}
                           [::tu/blocks '{a :i64, b [:list :i64]} in-vals]]
                          {:preserve-blocks? true
                           :with-col-types? true})))))
