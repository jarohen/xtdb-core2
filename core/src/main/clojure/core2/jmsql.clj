(ns core2.jmsql
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.qgm :as qgm])
  (:import [clojure.lang MapEntry]))

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

(defmethod plan-query :match [{:keys [clauses query]}]
  (let [plans (cond->> (->> clauses
                            (mapv (fn [{:keys [table cols]}]
                                    [:scan table (zipmap cols (repeat []))])))
                query (cons (plan-query query)))
        qs (->> plans
                (into {} (map-indexed
                          (fn [idx plan]
                            [(symbol (format "q%d" idx))
                             [:foreach plan]]))))
        var->cols (-> (for [[qid [_ plan]] qs
                            box-var (qgm/box-vars plan)]
                        (MapEntry/create
                         box-var [:column [qid box-var]]))
                      (->> (group-by key))
                      (update-vals #(into #{} (map val) %)))
        unify-preds (->> (vals var->cols)
                         (into [] (mapcat
                                   (fn [cols]
                                     (when (> (count cols) 1)
                                       (for [[c1 & cs] (->> cols
                                                            (iterate next)
                                                            (take-while next))
                                             c2 cs]
                                         [:call '= c1 c2]))))))
        box-head (->> var->cols (into {} (map (juxt key (comp first val)))))]

    [:select box-head unify-preds qs]))

(defn- unform-form [var->col form]
  (letfn [(unform* [[form-type form-arg]]
            (case form-type
              :literal [:literal form-arg]
              :symbol (var->col form-arg)
              :call (let [{:keys [f args]} form-arg]
                      (into [:call f] (mapv unform* args)))))]
    (unform* form)))

(defmethod plan-query :where [{:keys [clauses query]}]
  (let [plan (plan-query query)]
    (if (= :select (first plan))
      (let [[_ head preds qs] plan]
        [:select head
         (into preds
               (map (partial unform-form head))
               clauses)
         qs])

      [:select (->> (qgm/box-vars plan)
                    (into {} (map (juxt identity
                                        (fn [lv]
                                          [:column 'q lv])))))
       (->> clauses (mapv (partial unform-form (constantly 'q))))
       {'q plan}])))

(defmethod plan-query :project [{:keys [clauses query]}]
  (let [plan (plan-query query)
        var->col (->> (qgm/box-vars plan)
                      (into {} (map (juxt identity
                                          (fn [lv]
                                            [:column 'q lv])))))
        projections (->> clauses
                         (reduce
                          (fn [acc [p-type p-arg :as p]]
                            (case p-type
                              :star (into acc
                                          (map (juxt identity (fn [lv] [:column 'q lv])))
                                          (qgm/box-vars plan))
                              :column (-> acc (assoc p-arg [:column 'q p-arg]))
                              :extends (into acc
                                             (map (fn [[out-col form]]
                                                    [out-col [:extends (unform-form var->col form)]]))
                                             p-arg)))
                          {}))]
    [:select projections
     []
     {'q [:foreach plan]}]))

(defmethod plan-query :top [{:keys [top-params query]}]
  [:top top-params (plan-query query)])

(defmethod plan-query :order-by [{:keys [order-by-specs query]}]
  [:order-by (mapv (juxt :value :direction) order-by-specs)
   (plan-query query)])

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

