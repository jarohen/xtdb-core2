(ns core2.sql.tree-qgm.decorrelation
  (:require [clojure.set :as set]
            [clojure.zip :as z]
            [core2.rewrite :as r]
            [core2.sql.tree-qgm :as qgm]
            [clojure.string :as str])
  (:import (clojure.lang MapEntry)))

(defn qgm-sub-quantifiers [ag]
  (r/collect
   (fn [ag]
     (r/zcase ag
       (:qgm.quantifier/foreach :qgm.quantifier/all :qgm.quantifier/existential)
       [(:qgm.quantifier/id (second (z/node ag)))]

       nil))
   ag))

(defn expr-quantifiers [ag]
  (r/collect (fn [ag]
               (r/zmatch ag
                 [:column q _] #{q}
                 nil))
             set/union
             ag))

(defn box-quantifiers [ag]
  (let [[_ _opts & qs] (z/node ag)]
    (->> (for [[_ {qid :qgm.quantifier/id} :as q] qs]
           (MapEntry/create qid q))
         (into {}))))

(defn ag->preds [ag]
  (::qgm/preds (meta ag)))

;; TODO check how close this is to 'SELMERGE' from the Starburst-92 paper
(defn remove-identity-select-box [ag]
  (r/zmatch ag
    [:qgm.box/select box-opts
     [:qgm.quantifier/foreach q-opts inner-box]]
    (let [{body-cols :qgm.box.body/columns} box-opts
          {qid :qgm.quantifier/id} q-opts]
      ;; TODO check distinct=preserve etc, there's probably more conditions
      (when (and (not (contains? (->> (ag->preds ag)
                                      (into #{} (comp (map z/vector-zip)
                                                      (mapcat expr-quantifiers))))
                                 qid))
                 (every? #(= % :column) (map first body-cols)))
        (-> ag
            ;; TODO column mapping
            (z/replace inner-box))))

    nil))

(defn ->scalar-aggregate [ag]
  (r/zmatch ag
    [:qgm.quantifier/scalar scalar-opts
     [:qgm.box/grouping group-opts
      [:qgm.quantifier/foreach group-q-opts
       inner-box]]]
    (when (empty? (:qgm.box.grouping/grouping-by group-opts))
      {:scalar-opts scalar-opts
       :group-opts group-opts
       :group-q-opts group-q-opts
       :inner-box inner-box})

    nil))

(defn- qs-except [ag qid]
  (let [[_ _ & qs] (z/node ag)]
    (->> qs
         (filterv (fn [[_ {found-qid :qgm.quantifier/id} _]]
                    (and found-qid (not= found-qid qid)))))))

(defn- ->sym [& parts]
  (symbol (str/join "_" parts)))

(do
;; rule #9
  (defn- scalar-aggregate->apply-left-outer-join [ag]
    (r/zcase ag
      :qgm.box/select
      (when-let [{:keys [scalar-opts group-opts group-q-opts inner-box]} (->> ag (r/one-tu ->scalar-aggregate))]
        (let [scalar-qid (:qgm.quantifier/id scalar-opts)
              other-qs (qs-except ag scalar-qid)
              pf-qid (->sym scalar-qid "r9pf")
              col-mapping (->> (for [[_ {qid :qgm.quantifier/id, cols :qgm.quantifier/columns} _] other-qs
                                     col cols]
                                 (MapEntry/create [:column qid col]
                                                  [:column pf-qid (->sym qid col)]))
                               (into {}))]
          ;; TODO finish me
          (-> ag
              (z/replace [:qgm.box/select (second (z/node ag))
                          [:qgm.quantifier/foreach {}
                           [:qgm.box/grouping group-opts
                            [:qgm.quantifier/foreach {:qgm.quantifier/id (gensym scalar-qid)}
                             [:qgm.box/outer-join {}
                              [:qgm.quantifier/preserved-foreach {:qgm.quantifier/id pf-qid}
                               (into [:qgm.box/select {:qgm.box.head/columns (->> (vals col-mapping)
                                                                                  (mapv last))
                                                       :qgm.box.body/columns (mapv key col-mapping)}]
                                     other-qs)]
                              [:qgm.quantifier/foreach group-q-opts
                               inner-box]]]]]]))))

      nil))

  ;; SELECT c.custkey FROM customer c
  ;; WHERE 1000000 < (SELECT SUM(o.totalprice) FROM orders o WHERE o.custkey = c.custkey)
  (def scalar-subquery
    '{:tree [:qgm.box/select {:qgm.box.head/columns [custkey]
                              :qgm.box.body/columns [[:column q0 custkey]]}
             [:qgm.quantifier/foreach {:qgm.quantifier/id q0
                                       :qgm.quantifier/columns [custkey]}
              [:qgm.box/base-table {:qgm.box.base-table/name customer
                                    :qgm.box.head/columns [custkey]}]]

             [:qgm.quantifier/scalar {:qgm.quantifier/id q1
                                      :qgm.quantifier/columns [$column_1$]}
              [:qgm.box/select {:qgm.box.head/columns [$column_1$]
                                :qgm.box.body/columns [[:column q2 $column_1$]]}
               [:qgm.quantifier/foreach {:qgm.quantifier/id q2
                                         :qgm.quantifier/columns [$column_1$]}
                [:qgm.box/grouping {:qgm.box.head/columns [$column_1$]
                                    :qgm.box.body/columns [[:agg-call sum [:column q3 totalprice]]]}
                 [:qgm.quantifier/foreach {:qgm.quantifier/id q3
                                           :qgm.quantifier/columns [totalprice]}
                  [:qgm.box/select {:qgm.box.head/columns [totalprice]
                                    :qgm.box.body/columns [[:column q4 totalprice]]}
                   [:qgm.quantifier/foreach {:qgm.quantifier/id q4
                                             :qgm.quantifier/columns [custkey totalprice]}
                    [:qgm.box/base-table {:qgm.box.base-table/name orders
                                          :qgm.box.head/columns [custkey totalprice]}]]]]]]]]]

      :preds {p0 [:call = [:column q4 custkey] [:column q0 custkey]]
              p1 [:call < [:literal 1000000] [:column q1 $column_1$]]}})

  (-> (qgm/qgm-zip scalar-subquery)
      (->> (r/innermost (r/adhoc-tp (some-fn remove-identity-select-box
                                             scalar-aggregate->apply-left-outer-join)
                                    r/fail-tp)))
      (qgm/qgm-unzip)))

(def decorrelated-scalar-subquery
  '{:tree [:qgm.box/select {:qgm.box.head/columns [custkey]
                            :qgm.box.body/columns [[:column q0 custkey]]}
           [:qgm.quantifier/foreach {:qgm.quantifier/id q0}
            [:qgm.box/grouping {:qgm.box.head/columns [custkey $rownum $column_1$]
                                :qgm.box.body/columns [[:column q1 custkey]
                                                       [:column q1 $rownum]
                                                       [:agg-call sum [:column q1 totalprice]]]
                                :qgm.box.grouping/grouping-by [[:column q1 custkey]
                                                               [:column q1 $rownum]]}
             [:qgm.quantifier/foreach {:qgm.quantifier/id q1}
              [:qgm.box/outer-join {:qgm.box.head/columns [custkey $rownum totalprice]
                                    :qgm.box.body/columns [[:column q2 custkey]
                                                           [:column q2 $rownum]
                                                           [:column q4 totalprice]]}
               [:qgm.quantifier/preserved-foreach {:qgm.quantifier/id q2}
                [:qgm.box/select {:qgm.box.head/columns [custkey $rownum]
                                  :qgm.box.body/columns [[:column q2 custkey]
                                                         [:row-number]]}
                 [:qgm.quantifier/foreach {:qgm.quantifier/id q3
                                           :qgm.quantifier/columns [custkey]}
                  [:qgm.box/base-table {:qgm.box.base-table/name customer
                                        :qgm.box.head/columns [custkey]}]]]]

               [:qgm.quantifier/foreach {:qgm.quantifier/id q4
                                         :qgm.quantifier/columns [custkey totalprice]}
                [:qgm.box/select {:qgm.box.head/columns [custkey totalprice]
                                  :qgm.box.body/columns [[:column q5 custkey]
                                                         [:column q5 totalprice]]}
                 [:qgm.quantifier/foreach {:qgm.quantifier/id q5
                                           :qgm.quantifier/columns [custkey totalprice]}
                  [:qgm.box/base-table {:qgm.box.base-table/name order
                                        :qgm.box.head/columns [custkey totalprice]}]]]]]]]]]
    :preds {p0 [:call = [:column q2 custkey] [:column q4 custkey]]
            p1 [:call < [:literal 1000000] [:column q0 $column_1$]]}})
