(ns core2.datasets.tpch.jmsql)

(defn- with-params [q params]
  (vary-meta q assoc ::params params))

(def q1-pricing-summary-report
  (-> '[:order-by [l_returnflag l_linestatus]
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
          [:where (<= l_shipdate (- #time/date "1998-12-01" $delta))
           [:match [(lineitem [l_shipdate l_returnflag l_linestatus
                               l_quantity l_extendedprice l_discount
                               l_tax])]]]]]]
      (with-params {:delta #time/period "P90D"})))

(def q2-minimum-cost-supplier
  (-> '[:project [s_acctbal s_name
                  n_name
                  p_partkey p_mfgr
                  s_address s_phone s_comment]
        [:order-by [[s_acctbal :desc] n_name s_name p_partkey]
         [:where [(= ps_supplycost
                     (q [:using #{p}
                         [:group [{min_cost (min ps_supplycost)}]
                          [:match [(partsupp [{:ps_partkey p, :ps_suppkey s} ps_supplycost])
                                   (supplier {:id s, :s_nationkey n})
                                   (nation {:id n, :n_regionkey r})
                                   (region {:id r, :r_name $region})]]]]))
                  (like? p_type "%BRASS")]
          [:match [(part [{:id p, :p_size $size} p_type])
                   (partsupp [{:ps_partkey p, :ps_suppkey s} ps_supplycost])
                   (supplier [{:id s, :s_nationkey n}
                              s_acctbal s_name s_address s_phone s_comment])
                   (nation [{:id n, :n_regionkey r} n_name])
                   (region {:id r, :r_name $region})]]]]]
      (with-params {:region "EUROPE", :size 15})))

(def q3-shipping-priority
  (-> '[:order-by [[revenue :desc] o_orderdate]
        [:group [l_orderkey o_orderdate o_shippriority
                 {revenue (sum (* l_extendedprice (- 1 l_discount)))}]
         [:where [(< o_orderdate $date)
                  (> l_shipdate $date)]
          [:match [(customer {:id c, :c_mktsegment $segment})
                   (order [{:id o, :o_custkey c} o_orderdate o_shippriority])
                   (lineitem [{:l_orderkey o} l_shipdate l_extendedprice l_discount])]]]]]
      (with-params {:segment "BUILDING", :date #time/date "1995-03-15"})))

(def q4-order-priority-checking
  (-> '[:order-by [o_orderpriority]
        [:group [o_orderpriority {order_count (count *)}]
         [:where [(>= o_orderdate $date)
                  (< o_orderdate (+ $date #time/period "P3M"))
                  (exists? [:using #{o}
                            [:where [(< l_commitdate l_receiptdate)]
                             [:match [(lineitem [{:l_orderkey o} l_commitdate l_receiptdate])]]]])]
          [:match [{o (order [o_orderpriority o_orderdate])}]]]]]

      (with-params {:date #time/date "1993-07-01"})))

(def q5-local-supplier-volume
  (-> '[:order-by [[revenue :desc]]
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

#_
(def q6-forecasting-revenue-change
  (-> '[:group-by [{revenue (sum disc_price)}]
        [:project [{disc_price (* l_extendedprice l_discount)}]
         [:scan
          lineitem
          [{l_shipdate (and (>= l_shipdate ?start-date)
                            (< l_shipdate ?end-date))}
           l_extendedprice
           {l_discount (and (>= l_discount ?min-discount)
                            (<= l_discount ?max-discount))}
           {l_quantity (< l_quantity 24.0)}]]]]
      (with-params {'?start-date #time/date "1994-01-01"
                    '?end-date #time/date "1995-01-01"
                    '?min-discount 0.05
                    '?max-discount 0.07})))

#_
(def q7-volume-shipping
  (-> '[:order-by [[supp_nation] [cust_nation] [l_year]]
        [:group-by [supp_nation cust_nation l_year {revenue (sum volume)}]
         [:project [supp_nation cust_nation
                    {l_year (extract "YEAR" l_shipdate)}
                    {volume (* l_extendedprice (- 1 l_discount))}]
          [:rename {n1_n_name supp_nation, n2_n_name cust_nation}
           [:join [{c_nationkey n2_n_nationkey}
                   (or (and (= n1_n_name ?nation1)
                            (= n2_n_name ?nation2))
                       (and (= n1_n_name ?nation2)
                            (= n2_n_name ?nation1)))]
            [:join [{o_custkey c_custkey}]
             [:join [{s_nationkey n1_n_nationkey}]
              [:join [{l_orderkey o_orderkey}]
               [:join [{s_suppkey l_suppkey}]
                [:scan supplier [s_suppkey s_nationkey]]
                [:scan
                 lineitem
                 [l_orderkey l_extendedprice l_discount l_suppkey
                  {l_shipdate (and (>= l_shipdate ?start-date)
                                   (<= l_shipdate ?end-date))}]]]
               [:scan orders [o_orderkey o_custkey]]]
              [:rename n1
               [:scan nation [{n_name (or (= n_name ?nation1) (= n_name ?nation2))} n_nationkey]]]]
             [:scan customer [c_custkey c_nationkey]]]
            [:rename n2
             [:scan nation [{n_name (or (= n_name ?nation1) (= n_name ?nation2))} n_nationkey]]]]]]]]
      (with-params {'?nation1 "FRANCE"
                    '?nation2 "GERMANY"
                    '?start-date #time/date "1995-01-01"
                    '?end-date #time/date "1996-12-31"})))

#_
(def q8-national-market-share
  (-> '[:order-by [[o_year]]
        [:project [o_year {mkt_share (/ brazil_revenue revenue)}]
         [:group-by [o_year {brazil_revenue (sum brazil_volume)} {revenue (sum volume)}]
          [:project [{o_year (extract "YEAR" o_orderdate)}
                     {brazil_volume (if (= nation ?nation)
                                      (* l_extendedprice (- 1 l_discount))
                                      0.0)}
                     {volume (* l_extendedprice (- 1 l_discount))}
                     nation]
           [:rename {n2_n_name nation}
            [:join [{s_nationkey n2_n_nationkey}]
             [:join [{c_nationkey n1_n_nationkey}]
              [:join [{o_custkey c_custkey}]
               [:join [{l_orderkey o_orderkey}]
                [:join [{l_suppkey s_suppkey}]
                 [:join [{p_partkey l_partkey}]
                  [:scan part [p_partkey {p_type (= p_type ?type)}]]
                  [:scan lineitem [l_orderkey l_extendedprice l_discount l_suppkey l_partkey]]]
                 [:scan supplier [s_suppkey s_nationkey]]]
                [:scan
                 orders
                 [o_orderkey o_custkey
                  {o_orderdate (and (>= o_orderdate ?start-date)
                                    (<= o_orderdate ?end-date))}]]]
               [:scan customer [c_custkey c_nationkey]]]
              [:join [{r_regionkey n1_n_regionkey}]
               [:scan region [r_regionkey {r_name (= r_name ?region)}]]
               [:rename n1
                [:scan nation [n_name n_nationkey n_regionkey]]]]]
             [:rename n2
              [:scan nation [n_name n_nationkey]]]]]]]]]
      (with-params {'?nation "BRAZIL"
                    '?region "AMERICA"
                    '?type "ECONOMY ANODIZED STEEL"
                    '?start-date #time/date "1995-01-01"
                    '?end-date #time/date "1996-12-31"})))

#_
(def q9-product-type-profit-measure
  (-> '[:order-by [[nation] [o_year {:direction :desc}]]
        [:group-by [nation o_year {sum_profit (sum amount)}]
         [:rename {n_name nation}
          [:project [n_name
                     {o_year (extract "YEAR" o_orderdate)}
                     {amount (- (* l_extendedprice (- 1 l_discount))
                                (* ps_supplycost l_quantity))}]
           [:join [{s_nationkey n_nationkey}]
            [:join [{l_orderkey o_orderkey}]
             [:join [{l_suppkey s_suppkey}]
              [:join [{l_partkey ps_partkey} {l_suppkey ps_suppkey}]
               [:join [{p_partkey l_partkey}]
                [:scan part [p_partkey {p_name (like p_name "%green%")}]]
                [:scan
                 lineitem
                 [l_orderkey l_extendedprice l_discount l_suppkey l_partkey l_quantity]]]
               [:scan partsupp [ps_partkey ps_suppkey ps_supplycost]]]
              [:scan supplier [s_suppkey s_nationkey]]]
             [:scan orders [o_orderkey o_orderdate]]]
            [:scan nation [n_name n_nationkey]]]]]]]
      #_
      (with-params {'?color "green"})))

#_
(def q10-returned-item-reporting
  (-> '[:top {:limit 20}
        [:order-by [[revenue {:direction :desc}]]
         [:group-by [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                     {revenue (sum disc_price)}]
          [:project [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                     {disc_price (* l_extendedprice (- 1 l_discount))}]
           [:join [{c_nationkey n_nationkey}]
            [:join [{o_orderkey l_orderkey}]
             [:join [{c_custkey o_custkey}]
              [:scan customer [c_custkey c_name c_acctbal c_address c_phone c_comment c_nationkey]]
              [:scan
               orders
               [o_orderkey o_custkey
                {o_orderdate (and (>= o_orderdate ?start-date)
                                  (< o_orderdate ?end-date))}]]]
             [:scan
              lineitem
              [l_orderkey {l_returnflag (= l_returnflag "R")} l_extendedprice l_discount]]]
            [:scan nation [n_nationkey n_name]]]]]]]
      (with-params {'?start-date #time/date "1993-10-01"
                    '?end-date #time/date "1994-01-01"})))

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

(def q13-customer-distribution
  '[:order-by [[custdist :desc] [c_count :desc]]
    [:group [c_count {custdist (count *)}]
     [:group [c {c_count (count o)}]
      [:left-join [c (not (like? o_comment "%special%requests%"))]
       [:match [(customer {:id c})]]
       [:match [(order [{:o_custkey c} o_comment])]]]]]])

#_
(def q14-promotion-effect
  (-> '[:project [{promo_revenue (* 100 (/ promo_revenue revenue))}]
        [:group-by [{promo_revenue (sum promo_disc_price)}
                    {revenue (sum disc_price)}]
         [:project [{promo_disc_price (if (like p_type "PROMO%")
                                        (* l_extendedprice (- 1 l_discount))
                                        0.0)}
                    {disc_price (* l_extendedprice (- 1 l_discount))}]
          [:join [{p_partkey l_partkey}]
           [:scan part [p_partkey p_type]]
           [:scan
            lineitem
            [l_partkey l_extendedprice l_discount
             {l_shipdate (and (>= l_shipdate ?start-date)
                              (< l_shipdate ?end-date))}]]]]]]
      (with-params {'?start-date #time/date "1995-09-01"
                    '?end-date #time/date "1995-10-01"})))

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

#_
(def q16-part-supplier-relationship
  (-> '[:order-by [[supplier_cnt {:direction :desc}] [p_brand] [p_type] [p_size]]
        [:group-by [p_brand p_type p_size {supplier_cnt (count ps_suppkey)}]
         [:distinct
          [:project [p_brand p_type p_size ps_suppkey]
           [:join [{p_partkey ps_partkey}]
            [:semi-join [{p_size p_size}]
             [:scan
              part
              [p_partkey {p_brand (<> p_brand ?brand)} {p_type (not (like p_type "MEDIUM POLISHED%"))} p_size]]
             [:table ?sizes]]
            [:anti-join [{ps_suppkey s_suppkey}]
             [:scan partsupp [ps_partkey ps_suppkey]]
             [:scan supplier [s_suppkey {s_comment (like s_comment "%Customer%Complaints%")}]]]]]]]]
      (with-meta {::params {'?brand "Brand#45"
                            ;; '?type "MEDIUM POLISHED%"
                            }
                  ::table-args {'?sizes [{:p_size 49}
                                         {:p_size 14}
                                         {:p_size 23}
                                         {:p_size 45}
                                         {:p_size 19}
                                         {:p_size 3}
                                         {:p_size 36}
                                         {:p_size 9}]}})))

#_
(def q17-small-quantity-order-revenue
  (-> '[:project [{avg_yearly (/ sum_extendedprice 7)}]
        [:group-by [{sum_extendedprice (sum l_extendedprice)}]
         [:join [{p_partkey l_partkey} (< l_quantity small_avg_qty)]
          [:scan
           part
           [p_partkey {p_brand (= p_brand ?brand)} {p_container (= p_container ?container)}]]
          [:join [{l_partkey l_partkey}]
           [:project [l_partkey {small_avg_qty (* 0.2 avg_qty)}]
            [:group-by [l_partkey {avg_qty (avg l_quantity)}]
             [:scan lineitem [l_partkey l_quantity]]]]
           [:scan lineitem [l_partkey l_quantity]]]]]]
      (with-params {'?brand "Brand#23"
                    '?container "MED_BOX"})))

#_
(def q18-large-volume-customer
  (-> '[:top {:limit 100}
        [:order-by [[o_totalprice {:direction :desc}] [o_orderdate {:direction :desc}]]
         [:group-by [c_name c_custkey o_orderkey o_orderdate o_totalprice {sum_qty (sum l_quantity)}]
          [:join [{o_orderkey l_orderkey}]
           [:join [{o_custkey c_custkey}]
            [:semi-join [{o_orderkey l_orderkey}]
             [:scan orders [o_orderkey o_custkey o_orderdate o_totalprice]]
             [:select (> sum_qty ?qty)
              [:group-by [l_orderkey {sum_qty (sum l_quantity)}]
               [:scan lineitem [l_orderkey l_quantity]]]]]
            [:scan customer [c_name c_custkey]]]
           [:scan lineitem [l_orderkey l_quantity]]]]]]
      (with-params {'?qty 300})))

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

#_
(def q20-potential-part-promotion
  (-> '[:order-by [[s_name]]
        [:project [s_name s_address]
         [:semi-join [{s_suppkey ps_suppkey}]
          [:join [{n_nationkey s_nationkey}]
           [:scan nation [{n_name (= n_name ?nation)} n_nationkey]]
           [:scan supplier [s_name s_address s_nationkey s_suppkey]]]
          [:join [{ps_partkey l_partkey} {ps_suppkey l_suppkey} (> ps_availqty sum_qty)]
           [:semi-join [{ps_partkey p_partkey}]
            [:scan partsupp [ps_suppkey ps_partkey ps_availqty]]
            [:scan part [p_partkey {p_name (like p_name "forest%")}]]]
           [:project [l_partkey l_suppkey {sum_qty (* 0.5 sum_qty)}]
            [:group-by [l_partkey l_suppkey {sum_qty (sum l_quantity)}]
             [:scan lineitem
              [l_partkey l_suppkey l_quantity
               {l_shipdate (and (>= l_shipdate ?start-date)
                                (< l_shipdate ?end-date))}]]]]]]]]
      (with-params {;'?color "forest"
                    '?start-date #time/date "1994-01-01"
                    '?end-date #time/date "1995-01-01"
                    '?nation "CANADA"})))

#_
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

#_
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
   #_#'q6-forecasting-revenue-change
   #_#'q7-volume-shipping
   #_#'q8-national-market-share
   #_#'q9-product-type-profit-measure
   #_#'q10-returned-item-reporting
   #'q11-important-stock-identification
   #'q12-shipping-modes-and-order-priority
   #'q13-customer-distribution
   #_#'q14-promotion-effect
   #'q15-top-supplier
   #_#'q16-part-supplier-relationship
   #_#'q17-small-quantity-order-revenue
   #_#'q18-large-volume-customer
   #'q19-discounted-revenue
   #_#'q20-potential-part-promotion
   #_#'q21-suppliers-who-kept-orders-waiting
   #_#'q22-global-sales-opportunity])
