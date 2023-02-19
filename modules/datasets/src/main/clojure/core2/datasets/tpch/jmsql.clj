(ns core2.datasets.tpch.jmsql
  (:require [core2.jmsql :as jmsql]))

(defn- with-params [q params]
  (vary-meta q assoc ::params params))

(def q1-pricing-summary-report
  (-> '[:order [l_returnflag l_linestatus]
        [:group [l_returnflag l_linestatus
                 {sum_qty (sum l_quantity)
                  sum_base_price (sum l_extendedprice)
                  sum_disc_price (sum disc_price)
                  sum_charge (sum (* disc_price (+ 1 l_tax)))
                  avg_qty (avg l_quantity)
                  avg_price (avg l_extendedprice)
                  avg_disc (avg l_discount)
                  count_order (count *)}]
         [:project [* {disc_price (* l_extendedprice (- 1 l_discount))}]
          [:where [(<= l_shipdate (- #time/date "1998-12-01" $delta))]
           [:match [(lineitem [l_shipdate l_returnflag l_linestatus
                               l_quantity l_extendedprice l_discount
                               l_tax])]]]]]]
      (with-params {:delta #time/period "P90D"})))

(def q2-minimum-cost-supplier
  (-> '[:project [s_acctbal s_name
                  n_name
                  p_partkey p_mfgr
                  s_address s_phone s_comment]
        [:order [[s_acctbal :desc] n_name s_name p_partkey]
         [:where [(= ps_supplycost
                     (q #{p}
                        [:group [{min_cost (min ps_supplycost)}]
                         [:match [(partsupp [{:ps_partkey p, :ps_suppkey s} ps_supplycost])
                                  (supplier {:id s, :s_nationkey n})
                                  (nation {:id n, :n_regionkey r})
                                  (region {:id r, :r_name $region})]]]))
                  (like? p_type "%BRASS")]
          [:match [(part [{:id p, :p_size $size} p_type])
                   (partsupp [{:ps_partkey p, :ps_suppkey s} ps_supplycost])
                   (supplier [{:id s, :s_nationkey n}
                              s_acctbal s_name s_address s_phone s_comment])
                   (nation [{:id n, :n_regionkey r} n_name])
                   (region {:id r, :r_name $region})]]]]]
      (with-params {:region "EUROPE", :size 15})))

(def q3-shipping-priority
  (-> '[:order [[revenue :desc] o_orderdate]
        [:group [l_orderkey o_orderdate o_shippriority
                 {revenue (sum (* l_extendedprice (- 1 l_discount)))}]
         [:where [(< o_orderdate $date)
                  (> l_shipdate $date)]
          [:match [(customer {:id c, :c_mktsegment $segment})
                   (order [{:id o, :o_custkey c} o_orderdate o_shippriority])
                   (lineitem [{:l_orderkey o} l_shipdate l_extendedprice l_discount])]]]]]
      (with-params {:segment "BUILDING", :date #time/date "1995-03-15"})))

(def q4-order-priority-checking
  (-> '[:order [o_orderpriority]
        [:group [o_orderpriority {order_count (count *)}]
         [:where [(>= o_orderdate $date)
                  (< o_orderdate (+ $date #time/period "P3M"))
                  (exists? (q [:using #{o}
                               [:where [(< l_commitdate l_receiptdate)]
                                [:match [(lineitem [{:l_orderkey o} l_commitdate l_receiptdate])]]]]))]
          [:match [(order [{:id o} o_orderpriority o_orderdate])]]]]]

      (with-params {:date #time/date "1993-07-01"})))

(def q5-local-supplier-volume
  (-> '[:order [[revenue :desc]]
        [:group [n_name
                 {revenue (sum (* l_extendedprice (- 1 l_discount)))}]
         [:where [(>= o_orderdate $date)
                  (< o_orderdate (+ $date #time/period "P1Y"))]
          [:match [(customer {:id c, :c_nationkey n})
                   (order [{:id o, :o_custkey c} o_orderdate])
                   (lineitem {:l_orderkey o, :l_suppkey s})
                   (supplier {:id s, :s_nationkey n})
                   (nation [{:id n, :n_regionkey r} n_name])
                   (region {:id r, :r_name $region})]]]]]

      (with-params {:date #time/date "1994-01-01"})))

(def q6-forecasting-revenue-change
  (-> '[:group [{revenue (sum disc_price)}]
        [:project [{disc_price (* l_extendedprice l_discount)}]
         [:where [(>= l_shipdate ?start-date)
                  (< l_shipdate ?end-date)
                  (>= l_discount ?min-discount)
                  (<= l_discount ?max-discount)
                  (< l_quantity 24.0)]
          [:match [(lineitem [l_extendedprice l_shipdate l_discount l_quantity])]]]]]
      (with-params {'?start-date #time/date "1994-01-01"
                    '?end-date #time/date "1995-01-01"
                    '?min-discount 0.05
                    '?max-discount 0.07})))

(def q7-volume-shipping
  (-> '[:order [supp_nation cust_nation l_year]
        [:group [supp_nation cust_nation l_year
                 {revenue (sum volume)}]
         [:project [supp_nation cust_nation
                    {l_year (extract :year l_shipdate)
                     volume (* l_extendedprice (- 1 l_discount))}]
          [:where [(or (and (= supp_nation "FRANCE")
                            (= cust_nation "GERMANY"))
                       (and (= supp_nation "GERMANY")
                            (= cust_nation "FRANCE")))
                   (>= l_shipdate ?start-date)
                   (<= l_shipdate ?end-date)]
           [:match [(supplier {:id s, :s_nationkey n1})
                    (lineitem [{:l_suppkey s} l_extendedprice l_discount l_shipdate])
                    (orders {:id o, :o_custkey c})
                    (customer {:id c, :c_nationkey n2})
                    (nation {:id n1, :n_name supp_nation})
                    (nation {:id n2, :n_name cust_nation})]]]]]]
      (with-params {'?nation1 "FRANCE"
                    '?nation2 "GERMANY"
                    '?start-date #time/date "1995-01-01"
                    '?end-date #time/date "1996-12-31"})))

(def q8-national-market-share
  (-> '[:order [o_year]
        [:group [o_year {mkt_share (/ (sum brazil_volume) (sum volumet))}]
         [:project [{o_year (extract :year o_orderdate)
                     brazil_volume (if (= supp_nation $nation)
                                     (* l_extendedprice (- 1 l_discount))
                                     0.0)
                     volume (* l_extendedprice (- 1 l_discount))}
                    nation]
          [:match [(part {:id p, :p_type $type})
                   (supplier {:id s, :s_nationkey n2})
                   (lineitem [{:l_suppkey s, :l_partkey p}
                              l_extendedprice l_discount l_shipdate])
                   (orders [{:id o, :o_custkey c} o_orderdate])
                   (customer {:id c, :c_nationkey n1})
                   (nation {:id n1, :n_regionkey r})
                   (region {:id r, :r_name $region})
                   (nation {:id n2, :n_name supp_nation})]]]]]
      (with-params {:nation "BRAZIL"
                    :region "AMERICA"
                    :type "ECONOMY ANODIZED STEEL"
                    :start-date #time/date "1995-01-01"
                    :end-date #time/date "1996-12-31"})))

(def q9-product-type-profit-measure
  (-> '[:order [nation [o_year :desc]]
        [:group [nation
                 {o_year (extract :year o_orderdate)
                  sum_profit (sum (- (* l_extendedprice (- 1 l_discount))
                                     (* ps_supplycost l_quantity)))}]
         [:where [(like? p_name "%green%")]
          [:match [(part {:id p, :p_name p_name})
                   (partsupp {:ps_suppkey s, :ps_partkey p})
                   (supplier {:id s, :s_nationkey n})
                   (lineitem [{:l_suppkey s, :l_partkey p, :l_orderkey o}
                              l_extendedprice l_discount])
                   (orders [{:id o} o_orderdate])
                   (nation {:id n, :n_name nation})]]]]]
      #_
      (with-params {'?color "green"})))

(def q10-returned-item-reporting
  (-> '[:top {:limit 20}
        [:order [[revenue :desc]]
         [:group [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                  {revenue (sum (* l_extendedprice (- 1 l_discount)))}]
          [:where [(>= o_orderdate $date)
                   (< o_orderdate (+ $date #time/period "P3M"))]
           [:match [(customer [{:id c, :c_nationkey n}
                               c_name c_acctbal c_address c_phone c_comment])
                    (order [{:id o, :o_custkey c} o_orderdate])
                    (lineitem {:l_orderkey o, :l_returnflag "R"})
                    (nation [{:id n} n_name])]]]]]]
      (with-params {:date #time/date "1993-10-01"})))

(def q11-important-stock-identification
  (-> '[:order-by [[value :desc]]
        [:where [(> value (q [:group [{value (* $fraction (sum (* ps_supplycost ps_availqty)))}]
                              [:match [(partsupp [{:ps_suppkey s} ps_supplycost ps_availqty])
                                       (supplier {:id s, :s_nationkey n})
                                       (nation {:id n, :n_name $nation})]]]))]
         [:group [ps_partkey {value (sum (* ps_supplycost ps_availqty))}]
          [:match [(partsupp [{:ps_suppkey s} ps_supplycost ps_availqty])
                   (supplier {:id s, :s_nationkey n})
                   (nation {:id n, :n_name $nation})]]]]]

      (with-params {:nation "GERMANY", :fraction 0.0001})))

(def q12-shipping-modes-and-order-priority
  (-> '[:order-by [l_shipmode]
        [:group [l_shipmode
                 {high_line_count (sum (if (not-in? o_orderpriority ["1-URGENT" "2-HIGH"])
                                         1 0))
                  low_line_count (sum (if (in? o_orderpriority ["1-URGENT" "2-HIGH"])
                                        1 0))}]
         [:where [(in? l_shipmode $ship-modes)
                  (< l_commitdate l_receiptdate)
                  (< l_shipdate l_commitdate)
                  (>= l_receiptdate $date)
                  (< l_receiptdate (+ $date #time/period "P1Y"))]
          [:match [(order {:id o})
                   (lineitem [{:l_orderkey o} l_shipmode l_commitdate l_receiptdate l_shipdate])]]]]]

      (with-params {:ship-modes ["MAIL" "SHIP"]
                    :date #time/date "1994-01-01"})))

#_ ; TODO `:left-join`
;; do we want to include `c` or have it unified?
(def q13-customer-distribution
  '[:order-by [[custdist :desc] [c_count :desc]]
    [:group [c_count {custdist (count *)}]
     [:group [c {c_count (count o)}]
      [:left-join [c
                   (not (like? o_comment "%special%requests%"))]
       [:match [(customer {:id c})]]
       [:match [(order [{:o_custkey c} o_comment])]]]]]])

(def q14-promotion-effect
  (-> '[:group [{promo_revenue
                 (* 100 (/ (sum (if (like p_type "PROMO%")
                                  (* l_extendedprice (- 1 l_discount))
                                  0.0))
                           (sum (* l_extendedprice (- 1 l_discount)))))}]
        [:where [(and (>= l_shipdate $date)
                      (< l_shipdate (+ $date #time/period "P1M")))]
         [:match [(part [{:id p} p_type])
                  (lineitem [{:l_partkey p}
                             l_extendedprice l_discount l_shipdate])]]]]
      (with-params {:date #time/date "1995-09-01"})))

(def q15-top-supplier
  (-> '[:with [[revenue
                [:group [{supplier_no l_suppkey
                          total_revenue (sum (* l_extendedprice (- 1 l_discount)))}]
                 [:where [(>= l_shipdate $date)
                          (< l_shipdate (+ $date #time/period "P3M"))]
                  [:match [(lineitem [l_shipdate l_suppkey l_extendedprice l_discount])]]]]]]

        [:order-by [s]
         [:where (= total_revenue
                    (q [:group [{max_revenue (max total_revenue)}]
                        [:match [[_ (revenue {:keys [total_revenue]})]]]]))

          [:match [(supplier {:id s})
                   (revenue [{:supplier_no s} total_revenue])]]]]]
      (with-params {:date #time/date "1996-01-01"})))

(def q16-part-supplier-relationship
  (-> '[:order [[supplier_cnt :desc] p_brand p_type p_size]
        [:group [p_brand p_type p_size {supplier_cnt (count-distinct ps_suppkey)}]
         [:where [(<> p_brand $brand)
                  (in? p_size $sizes)
                  (not (like? p_type "MEDIUM POLISHED%"))
                  (not-in? ps_suppkey
                           (q [:project [s]
                               [:where [(not (like? s_comment "%Customer%Complaints%"))]
                                [:match [(supplier [{:id s} s_comment])]]]]))]
          [:match [(part [{:id p} p_brand p_type p_size p_suppkey])
                   (partsupp [{:ps_partkey p} ps_suppkey])]]]]]
      (with-params {:brand "Brand#45"
                    :sizes [49 14 23 45 19 3 36 9]})))

#_ ; TODO QGM for `:group`, and correlated sub-query
(def q17-small-quantity-order-revenue
  (-> '[:group [{avg_yearly (/ (sum l_extendedprice) 7)}]
        [:where [(< l_quantity (q #{p}
                                  [:group [(* 0.2 (avg l_quantity))]
                                   [:match [(lineitem [{:l_partkey p} l_quantity])]]]))]
         [:match [(lineitem [{:l_partkey p} l_quantity])
                  (part [{:id p, :p_brand $brand, :p_container $container}])]]]]
      (with-params {:brand "Brand#23"
                    :container "MED_BOX"})))

#_ ; TODO QGM for `:group`
(def q18-large-volume-customer
  (-> '[:top {:limit 100}
        [:order [[o_totalprice :desc] [o_orderdate :desc]]
         [:group [c_name c_custkey o_orderkey o_orderdate o_totalprice
                  {sum_qty (sum l_quantity)}]
          [:where [(in? o_orderkey
                        (q [:project [o]
                            [:where [(> sum_qty $qty)]
                             [:group [o {sum_qty (sum l_quantity)}]
                              [:match [(lineitem [{:l_orderkey o} l_quantity])]]]]]))]
           [:match [(customer [{:id c_custkey} c_name])
                    (orders [{:id o_orderkey, :o_custkey c_custkey}
                             o_orderdate o_totalprice])
                    (lineitem [{:l_orderkey o_orderkey} l_quantity])]]]]]]
      (with-params {:qty 300})))

(def q19-discounted-revenue
  (-> '[:group [{revenue (sum (* l_extendedprice (- 1 l_discount)))}]
        [:where [(in? l_shipmode ["AIR" "AIR REG"])
                 (or (and (= p_brand $brand1)
                          (in? p_container #{"SM CASE", "SM BOX", "SM PACK", "SM PKG"})
                          (between l_quantity $quantity1 (+ $quantity1 10))
                          (between p_size 1 5))
                     (and (= p_brand $brand2)
                          (in? p_container #{"MED BAG", "MED BOX", "MED PACK", "MED PKG"})
                          (between l_quantity $quantity2 (+ $quantity2 10))
                          (between p_size 1 10))
                     (and (= p_brand $brand3)
                          (in? p_container #{"LG CASE", "LG BOX", "LG PACK", "LG PKG"})
                          (between l_quantity $quantity3 (+ $quantity3 10))
                          (between p_size 1 15)))]
         [:match [(part [{:id p} p_size p_brand p_container])
                  (lineitem [{:l_partkey p, :l_shipinstruct "DELIVER IN PERSON"}
                             l_shipmode l_quantity])]]]]

      (with-params {:quantity1 1, :quantity2 10, :quantity3 20
                    :brand1 "Brand#12", :brand2 "Brand23", :brand3 "Brand#34"})))

#_ ; TODO correlated sub-query
(def q20-potential-part-promotion
  (-> '[:order [s_name]
        [:project [s_name s_address]
         [:where
          [(in? s
                (q #{s}
                   [:project [s]
                    [:where
                     [(> ps_availqty
                         (q #{p s}
                            [:group [(* 0.5 (sum l_quantity))]
                             [:where [(>= l_shipdate $date)
                                      (< l_shipdate (+ $date #time/period "P1Y"))]
                              [:match [(lineitem [{:l_partkey p, :l_suppkey s}
                                                  l_shipdate])]]]]))]

                     [:match [(partsupp [{:ps_suppkey s, :ps_partkey p}
                                         ps_availqty])]]]]))]

          [:match [(supplier {:id s, :s_nationkey n})
                   (nation [{:id n, :n_name $nation}])]]]]]

      (with-params {:date #time/date "1994-01-01"
                    :nation "CANADA"})))

#_ ; TODO `:assign`
(def q21-suppliers-who-kept-orders-waiting
  (-> '[:assign [L1 [:join [{l1_l_orderkey l2_l_orderkey} (<> l1_l_suppkey l2_l_suppkey)]
                     [:join [{l1_l_suppkey s_suppkey}]
                      [:join [{l1_l_orderkey o_orderkey}]
                       [:rename l1
                        [:select (> l_receiptdate l_commitdate)
                         [:scan lineitem [l_orderkey l_suppkey l_receiptdate l_commitdate]]]]
                       [:scan orders [o_orderkey {o_orderstatus (= o_orderstatus "F")}]]]
                      [:semi-join [{s_nationkey n_nationkey}]
                       [:scan supplier [s_nationkey s_suppkey s_name]]
                       [:scan nation [n_nationkey {n_name (= n_name ?nation)}]]]]
                     [:rename l2
                      [:scan lineitem [l_orderkey l_suppkey]]]]]
        [:top {:limit 100}
         [:order-by [[numwait {:direction :desc}] [s_name]]
          [:group-by [s_name {numwait (count-star)}]
           [:distinct
            [:project [s_name l1_l_orderkey]
             [:anti-join [{l1_l_orderkey l3_l_orderkey}]
              L1
              [:join [{l1_l_orderkey l3_l_orderkey} (<> l3_l_suppkey l1_l_suppkey)]
               L1
               [:select (> l3_l_receiptdate l3_l_commitdate)
                [:rename l3
                 [:scan lineitem [l_orderkey l_suppkey l_receiptdate l_commitdate]]]]]]]]]]]]
      (with-params {'?nation "SAUDI ARABIA"})))

#_ ; TODO `:assign`
(def q22-global-sales-opportunity
  (-> '[:assign [Customer [:semi-join [{cntrycode cntrycode}]
                           [:project [c_custkey {cntrycode (substring c_phone 1 2 true)} c_acctbal]
                            [:scan customer [c_custkey c_phone c_acctbal]]]
                           [:table ?cntrycodes]]]
        [:order-by [[cntrycode]]
         [:group-by [cntrycode {numcust (count-star)} {totacctbal (sum c_acctbal)}]
          [:anti-join [{c_custkey o_custkey}]
           [:join [(> c_acctbal avg_acctbal)]
            Customer
            [:group-by [{avg_acctbal (avg c_acctbal)}]
             [:select (> c_acctbal 0.0)
              Customer]]]
           [:scan orders [o_custkey]]]]]]
      (with-meta {::table-args {'?cntrycodes [{:cntrycode "13"}
                                              {:cntrycode "31"}
                                              {:cntrycode "23"}
                                              {:cntrycode "29"}
                                              {:cntrycode "30"}
                                              {:cntrycode "18"}
                                              {:cntrycode "17"}]}})))

(def queries
  [#'q1-pricing-summary-report
   #'q2-minimum-cost-supplier
   #'q3-shipping-priority
   #'q4-order-priority-checking
   #'q5-local-supplier-volume
   #'q6-forecasting-revenue-change
   #'q7-volume-shipping
   #'q8-national-market-share
   #'q9-product-type-profit-measure
   #'q10-returned-item-reporting
   #'q11-important-stock-identification
   #'q12-shipping-modes-and-order-priority
   #_#'q13-customer-distribution
   #'q14-promotion-effect
   #'q15-top-supplier
   #'q16-part-supplier-relationship
   #_#'q17-small-quantity-order-revenue
   #_#'q18-large-volume-customer
   #'q19-discounted-revenue
   #_#'q20-potential-part-promotion
   #_#'q21-suppliers-who-kept-orders-waiting
   #_#'q22-global-sales-opportunity])
