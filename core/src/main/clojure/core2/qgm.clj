(ns core2.qgm
  (:require [clojure.spec.alpha :as s]))

(s/check-asserts true)

(defmulti box-spec first)

(s/def ::expression vector?)
(s/def ::column simple-symbol?)
(s/def ::table simple-symbol?)

(s/def ::box (s/multi-spec box-spec identity))
(s/def ::box-head (s/map-of ::column ::expression))

(s/def ::qid simple-symbol?)
(s/def ::quantifier-type #{:foreach :outer-foreach :any :all :anti})
(s/def ::quantifier (s/tuple ::quantifier-type ::box))

(s/def ::predicates (s/coll-of ::expression, :kind vector?))

(defmethod box-spec :scan [_]
  (s/tuple #{:scan}, ::table, (s/map-of ::column ::predicates)))

(defmethod box-spec :join [_]
  (s/tuple #{:join}, ::box-head, ::predicates, (s/map-of ::qid ::quantifier)))

(defmethod box-spec :group [_]
  (s/tuple #{:group}, (s/map-of ::column ::expression), ::box))

(defmulti plan-box first)

(defn- wrap-scan-col-pred [col preds]
  (case (count preds)
    0 col
    1 {col (first preds)}
    {col (list* 'and preds)}))

(defmethod plan-box :scan [[_ table cols]]
  [:scan table
   (vec (for [[col col-preds] cols]
          (-> col (wrap-scan-col-pred col-preds))))])

(defmethod plan-box :join [[_ _head _preds quantifiers]]
  ;; TODO projection/selection
  ;; TODO different q-types
  (->> quantifiers
       (map (fn [[_qid [_qid-type box]]]
              (plan-box box)))
       (reduce (fn [acc plan]
                 [:join [] acc plan]))))

(defmethod plan-box :group [[_ grouping-spec box]]
  ;; TODO
  [:group-by []
   box])

(comment
  (plan-box (doto '[:group {foo [:grouping-key [:column foo]]
                            foo-count [:call * [:literal 2] [:aggregate :sum [:grouping-key [:column foo]]]]}
                    [:join {foo [:column q0 foo]}
                     []
                     {q0 [:foreach
                          [:scan users
                           {id [[:call = [:column id] [:param ?id]]]
                            foo []}]]
                      q1 [:foreach
                          [:scan users
                           {id []}]]}]]
             (->> (s/assert ::box)))))
