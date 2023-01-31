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

(s/def ::project-clause
  (s/or :star #{'*}
        :column ::column
        :extends (s/map-of ::column ::form, :conform-keys true)))

(defmethod query-spec :project [_]
  (s/cat :op #{:project}
         :clauses (s/and vector? (s/spec (s/* ::project-clause)))
         :query (s/? ::query)))

(s/def ::limit nat-int?)
(s/def ::offset nat-int?)

(defmethod query-spec :top [_]
  (s/cat :op #{:top}
         :top-params (s/keys :opt-un [::limit ::offset])
         :query ::query))

(s/def ::order-by-value ::column)

(s/def ::order-by-spec
  (s/and (s/or :value ::order-by-value
               :value+direction (s/cat :value ::order-by-value
                                       :direction #{:asc :desc}))
         (s/conformer (fn [[obs-type obs-arg]]
                        (case obs-type
                          :value {:value obs-arg, :direction :asc}
                          :value+direction obs-arg))
                      (fn [v] [:value+direction v]))))

(defmethod query-spec :order-by [_]
  (s/cat :op #{:order-by}
         :order-by-specs (s/and vector? (s/spec (s/* ::order-by-spec)))
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
  (-> (case (count predicates)
        0 plan
        1 [:select (first predicates) plan]
        [:select (list* 'and predicates) plan])
      (with-meta (meta plan))))

(defn- wrap-unify [plan plans]
  (let [var->cols (-> plans
                      (->> (mapcat (comp :var->col meta))
                           (group-by key))
                      (update-vals #(into #{} (map val) %))
                      (into {}))]
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
        (vary-meta assoc :var->col (into {} (map (juxt identity identity) (keys var->cols)))))))

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
          (wrap-unify plans))))

(defn- unform-form [var->col form]
  (letfn [(unform* [[form-type form-arg]]
            (case form-type
              :literal form-arg
              :symbol (var->col form-arg)
              :call (let [{:keys [f args]} form-arg]
                      (list* f (mapv unform* args)))))]
    (unform* form)))

(defmethod plan-query :where [{:keys [clauses query]}]
  (let [plan (plan-query query)
        {:keys [var->col]} (meta plan)]
    (-> plan
        (wrap-select (for [clause clauses]
                       (unform-form var->col clause))))))

(defmethod plan-query :project [{:keys [clauses query]}]
  (let [plan (plan-query query)
        {:keys [var->col]} (meta plan)
        projections (->> clauses
                         (reduce (fn [acc [p-type p-arg :as p]]
                                   (case p-type
                                     :star (into acc
                                                 (map (juxt identity (fn [lv] [:column lv])))
                                                 (keys var->col))
                                     :column (-> acc (assoc p-arg p))
                                     :extends (into acc
                                                    (map (fn [[out-col form]]
                                                           [out-col [:extends form]]))
                                                    p-arg)))
                                 {}))]
    (-> [:project (->> projections
                       (mapv (fn [[out-col [p-type p-arg]]]
                               {out-col
                                (case p-type
                                  :column p-arg
                                  :extends (unform-form var->col p-arg))})))
         plan]
        (with-meta {:var->col (into {} (map (juxt key key)) projections)}))))

(defmethod plan-query :top [{:keys [top-params query]}]
  (let [plan (plan-query query)]
    (-> [:top top-params plan]
        (with-meta (meta plan)))))

(defmethod plan-query :order-by [{:keys [order-by-specs query]}]
  (let [plan (plan-query query)]
    (-> [:order-by (mapv (juxt :value :direction) order-by-specs)
         plan]
        (with-meta (meta plan)))))

(comment
  (compile-query '[:order-by [id]
                   [:top {:limit 10}
                    [:project [* id {id2 (* id 2)}]
                     [:where [(> id 4)
                              (< uid 2)]
                      [:match [(users [uid])
                               (products [id])]]]]]]))

(defn compile-query [query]
  (-> (conform-query query)
      (plan-query)))


;;;; QGM notes

'
[:mega-join [(= q0_b q1_b) (= q1_a q2_a)]
 {q0 [:foreach [:scan r1 [b]]]
  q1 [:foreach [:scan r2 [a b]]]
  q2 [:preserving-foreach [:scan r3 [a]]]}]

'
[:mega-join [(= q0_b q1_b) (= q1_a q2_a)]
 [[:scan r1 [b]]
  [:scan r2 [a b]]
  [:scan r3 [a]]]]

;; IIRC:
;; * no P quantifiers in a select box - these have to be in an specific outer join box
;; * in an outer-join box, maximum one F quantifier (zero for FOJ)

"SELECT * FROM r1 JOIN r2 USING (b) LEFT JOIN r3 USING (a) LEFT JOIN r4 USING (c)"

'
[:outer-join {a q2.a, b q2.b, c q3.c}
 [(= q2.a q3.a) (= q3.c q4.c)]
 {q2 [:foreach
      [:select {a q0.a, b q1.b}
       [(= q0.b q1.b)]
       {q0 [:foreach [:base r1 [b]]]
        q1 [:foreach [:base r2 [a b]]]}]]
  q3 [:preserving-foreach
      [:base r3 [a c]]]
  q4 [:preserving-foreach
      [:base r4 [c]]]}]
