(ns core2.jmsql
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]))

(defmulti query-spec first)
(defmulti plan-query :op)

(s/def ::column simple-symbol?)
(s/def ::table simple-symbol?)

(s/def ::query
  (s/multi-spec query-spec (fn [v t] [t v])))

(s/def ::match-clause
  (s/and list?
         (s/cat :table ::table
                :cols (s/and vector?
                             (s/spec (s/* ::column))))))

(defmethod query-spec :match [_]
  (s/cat :op #{:match}
         :clauses (s/and vector? (s/spec (s/* ::match-clause)))
         :query (s/? ::query)))

(s/def ::call
  (s/and list?
         (s/cat :f simple-symbol?
                :args (s/* ::form))))

(s/def ::form
  (s/or :literal (some-fn number? string? keyword?)
        :symbol simple-symbol?
        :call (s/spec ::call)))

(s/def ::where-clause ::form)

(defmethod query-spec :where [_]
  (s/cat :op #{:where}
         :clauses (s/and vector? (s/spec (s/* ::where-clause)))
         :query ::query))

(defn- conform-query [query]
  (let [conformed-query (s/conform ::query query)]
    (when (s/invalid? conformed-query)
      (throw (err/illegal-arg :malformed-query
                              {::err/message "Malformed query"
                               :query query
                               :explain (s/explain-data ::query query)})))
    conformed-query))

(defn- col-sym
  ([col]
   (vary-meta (symbol col) assoc :column? true))
  ([prefix col]
   (col-sym (str prefix col))))

(defn- wrap-select [plan predicates]
  (case (count predicates)
    0 plan
    1 [:select (first predicates) plan]
    [:select (list* 'and predicates) plan]))

(defn- unify-plans [plans]
  (let [var->cols (-> plans
                      (->> (mapcat (fn [clause-plan]
                                     (:var->col (meta clause-plan))))
                           (group-by key))
                      (update-vals #(into #{} (map val) %))
                      (into {}))]

    {:var->cols var->cols
     :var->col (->> var->cols
                    (into {} (map (juxt key (comp first val)))))}))

(defn- wrap-unify [plan {:keys [var->cols var->col]}]
  (-> [:project (vec (for [[lv cols] var->cols]
                       (if (contains? cols lv)
                         lv
                         {lv (first cols)})))
       (-> plan
           (wrap-select (vec
                         (for [cols (vals var->cols)
                               :when (> (count cols) 1)
                               ;; this picks an arbitrary binary order if there are >2
                               ;; once mega-join has multi-way joins we could throw the multi-way `=` over the fence
                               [c1 c2] (partition 2 1 cols)]
                           (list '= c1 c2)))))]
      (vary-meta assoc :var->col var->col)))

(defmethod plan-query :match [{:keys [clauses query]}]
    (let [clause-plans (->> clauses
                            (mapv (fn [{:keys [table cols]}]
                                    (let [var->col (->> cols
                                                        (into {} (map (juxt identity identity))))]
                                      (-> [:scan '$ table (mapv col-sym cols)]
                                          (vary-meta assoc :var->col var->col))))))

          plans (->> (cond-> clause-plans
                       query (conj (plan-query query)))

                     (into [] (map-indexed
                               (fn [idx plan]
                                 (let [prefix (symbol (format "m%d_" idx))
                                       var->col (->> (:var->col (meta plan))
                                                     (into {} (map (juxt key (comp (partial col-sym prefix) val)))))]
                                   (-> [:rename prefix plan]
                                       (with-meta {:var->col var->col})))))))]
      (-> [:mega-join [] plans]
          (wrap-unify (unify-plans plans)))))

(defmethod plan-query :where [{:keys [clauses query]}]
  (let [plan (plan-query query)
        {:keys [var->col]} (meta plan)]
    (-> plan
        (wrap-select (for [clause clauses]
                       (letfn [(unform [[form-type form-arg]]
                                 (case form-type
                                   :literal form-arg
                                   :symbol (var->col form-arg)
                                   :call (let [{:keys [f args]} form-arg]
                                           (list* f (map unform args)))))]
                         (unform clause)))))))

(comment
  (compile-query '[:where [(> id 4)
                           (< id 2)]
                   [:match [(users [id])
                            (products [id])]]]))

(defn compile-query [query]
  (-> (conform-query query)
      (plan-query)))
