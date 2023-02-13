(ns core2.jmsql
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.qgm :as qgm])
  (:import [clojure.lang MapEntry]))

(defmulti query-spec first)
(defmulti plan-query :op)

(s/def ::column simple-symbol?)
(s/def ::table simple-symbol?)
(s/def ::attr keyword?)
(s/def ::literal any?)
(s/def ::logic-var simple-symbol?)

(s/def ::query
  (s/multi-spec query-spec (fn [v t] [t v])))

(s/def ::match-value
  (s/or :logic-var ::logic-var, :literal ::literal))

(s/def ::single-map-match
  (-> (s/map-of ::attr ::match-value)
      (s/and (s/conformer vec #(into {} %)))))

(s/def ::match-cols
  (-> (s/or :single-map ::single-map-match
            :vector (-> (s/or :column ::column
                              :map (s/map-of ::attr ::match-value))
                        (s/and (s/conformer (fn [[tag arg]]
                                              (case tag :map arg, :column {(keyword arg) [:logic-var arg]}))
                                            (fn [arg]
                                              [:map arg])))
                        (s/coll-of :kind vector?)))

      (s/and (s/conformer (fn [[tag arg]]
                            (case tag
                              :single-map (vec arg)
                              :vector (into [] cat arg)))
                          (fn [v]
                            [:vector (mapv #(conj {} %) v)])))))

(s/def ::at-app-time ::literal)
(s/def ::app-time-in (s/tuple (s/nilable ::literal) (s/nilable ::literal)))
(s/def ::at-sys-time ::literal)
(s/def ::sys-time-in (s/tuple (s/nilable ::literal) (s/nilable ::literal)))

(s/def ::match-opts
  (s/keys :opt-un [::at-app-time ::app-time-in ::at-sys-time ::sys-time-in]))

(s/def ::match-clause
  (s/and list?
         (s/cat :table ::table,
                :match-cols ::match-cols
                :opts (s/? ::match-opts))))

(defmethod query-spec :match [_]
  (s/cat :op #{:match}
         :clauses (s/coll-of ::match-clause, :kind vector?)
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
         :clauses (s/coll-of ::where-clause, :kind vector?)
         :query ::query))

(s/def ::project-clause
  (s/or :star #{'*}
        :column ::column
        :extends (s/map-of ::column ::form, :conform-keys true)))

(defmethod query-spec :project [_]
  (s/cat :op #{:project}
         :clauses (s/coll-of ::project-clause, :kind vector?)
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
         (s/conformer (fn [[obs-tag obs-arg]]
                        (case obs-tag
                          :value {:value obs-arg, :direction :asc}
                          :value+direction obs-arg))
                      (fn [v] [:value+direction v]))))

(defmethod query-spec :order-by [_]
  (s/cat :op #{:order-by}
         :order-by-specs (s/coll-of ::order-by-spec, :kind vector?)
         :query ::query))

(defn- conform-query [query]
  (let [conformed-query (s/conform ::query query)]
    (when (s/invalid? conformed-query)
      (throw (err/illegal-arg :malformed-query
                              {::err/message "Malformed query"
                               :query query
                               :explain (s/explain-data ::query query)})))
    conformed-query))

(defn- ->unify-preds [var->cols]
  (->> (vals var->cols)
       (into [] (mapcat
                 (fn [cols]
                   (for [[c1 & cs] (iterate next cols)
                         :while cs
                         c2 cs]
                     [:call '= c1 c2]))))))

(defn- plan-match-clause [{:keys [table match-cols]}]
  (let [match-cols (->> match-cols
                        (mapv (fn [[a v]]
                                (MapEntry/create [:column (symbol a)] v))))
        var->cols (-> match-cols
                      (->> (keep (fn [[a [v-tag v-arg]]]
                                   (when (= v-tag :logic-var)
                                     (MapEntry/create v-arg a))))
                           (group-by key))
                      (update-vals vals))]

    [:scan (symbol table) (-> var->cols (update-vals first))
     (vec (concat (->> match-cols
                       (keep (fn [[a [v-tag :as v]]]
                               (when (= :literal v-tag)
                                 [:call '= a v]))))
                  (->unify-preds var->cols)))]))

(defmethod plan-query :match [{:keys [clauses query]}]
  (let [plans (cond->> (mapv plan-match-clause clauses)
                query (cons (plan-query query)))
        qs (->> plans
                (into {} (map-indexed
                          (fn [idx plan]
                            [(symbol (format "q%d" idx))
                             [:foreach plan]]))))
        var->cols (-> (for [[qid [_ plan]] qs
                            box-var (qgm/box-vars plan)]
                        (MapEntry/create box-var [:column [qid box-var]]))
                      (->> (group-by key))
                      (update-vals vals))]

    [:select (-> var->cols (update-vals first))
     (->unify-preds var->cols)
     qs]))

(defn- unform-form [var->col form]
  (letfn [(unform* [[form-tag form-arg]]
            (case form-tag
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
                          (fn [acc [p-tag p-arg]]
                            (case p-tag
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

