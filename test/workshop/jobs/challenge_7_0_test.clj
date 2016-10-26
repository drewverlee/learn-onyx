(ns workshop.jobs.challenge-7-0-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-7-0 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; We demonstrated that we can perform a transofmration on a single segment using 
;; a plain clojure function.

;; What we have used, but not covered is how to perform a transformation over multiple
;; segments in a fault tolerant manor.
;; Recall from challenge 6_1 we used the sum aggregation 

;;  :window/aggregation [:onyx.windowing.aggregation/sum :bytes-sent]

;; to find out the sum of the bytes-sent and then outputted that sum.  
;; This exercise is identical to that one. Only this time were highlighting (by dropping the code into the project) how
;; how the sum function is built. And in later exercises will show how to build your own.

;; Onyx ships with some aggregation functions out of the box. You read about them here:
;;  http://www.onyxplatform.org/docs/user-guide/0.9.11/#aggregation-state-management 


;; However these exercises are going to make it clear that your not limited to just
;; those. Writing your own is simple and yet again you will be able to use plain clojure
;; functions.

;; Try it with:
;;
;; `lein test workshop.jobs.challenge-7-0-test`
;;

(def input
  [{:event-id 1 :event-time #inst "2015-11-20T02:59:00.000-00:00" :bytes-sent 400}
   {:event-id 2 :event-time #inst "2015-11-20T06:58:00.000-00:00" :bytes-sent 50}
   {:event-id 3 :event-time #inst "2015-11-20T09:04:00.000-00:00" :bytes-sent 320}
   {:event-id 4 :event-time #inst "2015-11-20T04:00:00.000-00:00" :bytes-sent 1560}
   {:event-id 5 :event-time #inst "2015-11-20T06:05:00.000-00:00" :bytes-sent 350}
   {:event-id 6 :event-time #inst "2015-11-20T03:00:00.000-00:00" :bytes-sent 120}
   {:event-id 7 :event-time #inst "2015-11-20T07:23:00.000-00:00" :bytes-sent 640}
   {:event-id 8 :event-time #inst "2015-11-20T04:26:00.000-00:00" :bytes-sent 560}
   {:event-id 9 :event-time #inst "2015-11-20T01:41:59.000-00:00" :bytes-sent 1024}])

(def expected-output
  {[#inst "2015-11-20T00:00:00.000-00:00"
    #inst "2015-11-20T01:59:59.999-00:00"]
   1024

   [#inst "2015-11-20T02:00:00.000-00:00"
    #inst "2015-11-20T03:59:59.999-00:00"]
   520

   [#inst "2015-11-20T04:00:00.000-00:00"
    #inst "2015-11-20T05:59:59.999-00:00"]
   2120

   [#inst "2015-11-20T06:00:00.000-00:00"
    #inst "2015-11-20T07:59:59.999-00:00"]
   1040

   [#inst "2015-11-20T08:00:00.000-00:00"
    #inst "2015-11-20T09:59:59.999-00:00"]
   320})

(deftest test-level-6-challenge-1
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)
        n-trigger-fires (atom 0)
        p (promise)]

    (reset! c/fired-window-state {})

    (with-test-env
      [test-env [n-peers env-config peer-config]]

      (add-watch c/fired-window-state :watcher
                 (fn [k r old new]
                   (let [n (swap! n-trigger-fires inc)]
                     ;; This time, we have 5 discete
                     ;; buckets - so we wait for 5 updates.
                     (when (= n 5)
                       (deliver p true)))))
      
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :windows c/windows
                 :triggers c/triggers
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        @p
        (is (= expected-output @c/fired-window-state))))))
