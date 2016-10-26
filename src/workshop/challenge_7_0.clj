(ns workshop.challenge-7-0
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :bucket-page-views]])

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/plugin :onyx.plugin.core-async/input
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}
      
      {:onyx/name :bucket-page-views
       :onyx/plugin :onyx.peer.function/function
       :onyx/fn :clojure.core/identity
       :onyx/type :output
       :onyx/medium :function
       :onyx/uniqueness-key :event-id
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Identity function, used for windowing segments unchanged."}]))

;;; Lifecycles ;;;

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.challenge-7-0/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}])


(def windows
  [{:window/id :collect-segments
    :window/task :bucket-page-views
    :window/type :fixed
    :window/aggregation [::sum :bytes-sent]
    :window/window-key :event-time
    :window/range [2 :hours]
    :window/doc "Sums byte values in two hour fixed windows."}])


(def triggers
  [{:trigger/window-id :collect-segments
    :trigger/refinement :onyx.refinements/accumulating
    :trigger/on :onyx.triggers/segment
    :trigger/fire-all-extents? true
    :trigger/threshold [10 :elements]
    :trigger/sync ::deliver-promise!
    :trigger/doc "Fires against all extents every 10 segments processed."}])

(def fired-window-state (atom {}))

(defn deliver-promise! [event window {:keys [trigger/window-id] :as trigger} {:keys [lower-bound upper-bound] :as state-event} state]
  (let [lower (java.util.Date. lower-bound)
        upper (java.util.Date. upper-bound)]
    (swap! fired-window-state assoc [lower upper] state))
  )

;; BEGIN READ ME

;; Aggregations are a way to collect state about a stream in a fault tolerant manor. 

;; If you think of time as a line, then windows describe where along that line you want events to be aggregated into state.

;; e.g
;; 1pm -------------------- 2pm
;; |            |         |
;;  wwindow 1    window 2

;; triggers & watermarks describe when to do something with the state you have aggregated. 
;; e.g every hour emit all the state we have collected.

;; So aggregations describe what the transformation on the aggregation might be.
;; aggregations might be sum, min, max, average, etc...

;; our previous example

;; e.g every hour emit all the state we have collected.
;; used an aggregation, it was simple conj (collecting all the state and emit it as is)
;; so another example might be...

;; ever hour *sum* all the segments we have collected and emit them.

;; You will notice their is a nice separation of concerns inheirent in breaking things up this way

;; Window      : where
;; Triggers    : When
;; Aggregation : What
;; Accumulation: How (not covered)

;; The Where, When, How What idiom is stolen from here: https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102


;; Onyx exposes the ability to for you to create your own aggregations. To do this, you will need
;; to supply onyx several functions that work together to build up the aggregation. These functions are described tersely below but will walk through
;; each one.


; Key                               | Optional? | Description
; :aggregation/init                 | true      | Fn (window) to initialise the state.
; :aggregation/create-state-update  | false     | Fn (window, state, segment) to generate a serializable state machine update.
; :aggregation/apply-state-update   | false     | Fn (window, state, entry) to apply state machine update entry to a state.
; :aggregation/super-aggregation-fn | true      | Fn (window, state-1, state-2) to combine two states in the case of two windows being merged.

;; so imagine segments flowing into onyx
;; --- 5 --- 6 ----- onxy

;;  if i want to find the sum of these segments then i need to have some initial state built up 

;; --- 5 --- 6 ----- onxy
;; current-sum: 0

(defn sum-aggregation-fn-init [window]
  0)

;; now as these segments arrive in onyx ill need to describe how to aggregate them together.
;; Sense were trying to sum them up, we simply add them 

;; --- 5 --- 6 ----- onxy
;; current-sum: 0

;; --- 5 ---  onxy ---- 6
;; current-sum: 0 + 6

;; ------  onxy ---- 5 ---- 6
;; current-sum: 0 + 6 + 5


(defn sum-aggregation-fn [window state segment]
  (let [k (second (:window/aggregation window))]
    (+ state (get segment k))))

;; The above strategy strategy works for windows whose lengths are defined at compile time. But for
;; dynamic windows like sessions (http://www.onyxplatform.org/docs/user-guide/0.9.11/#_session_windows)
;; we need to provide a way to stitch together two collected aggregations that we now realize should be one.
;; If your wondering why we cant use the same logic as our aggregation-fn above, consider that the average of 
;; averages isn't correct. However, in our example below the sum of two sums is correct.

(defn sum-super-aggregation [window state-1 state-2]
  (+ state-1 state-2))

;; Finally we need to do some bookkeeping in order to be fault tolerant.
;; In this case we want to let other peers/servers know about our progress in the aggregation
;; for example that the current-sum is 5. So that if the peer doing the aggregation crashes
;; they can recover it.

;; Sense were doing a sum, we just need to keep them informed of the current sum


;; --- 5 --- 6 ----- onxy
;; current-sum: 0
;; tell another peer the sum is 0! <--- this is what v is in the function below

;; --- 5 ---  onxy ---- 6
;; current-sum: 0 + 6
;; tell another peer the sum is 6 <--- this is what v is in the function below

;; ------  onxy ---- 5 ---- 6
;; current-sum: 0 + 6 + 5
;; tell another peer the sum is 11 <--- this is what v is in the function below

(defn set-value-aggregation-apply-log [window state v]
  v)

;; All these functions get wrapped up so they can be referenced else where as a package

(def sum
  {:aggregation/init sum-aggregation-fn-init
   :aggregation/create-state-update sum-aggregation-fn
   :aggregation/apply-state-update set-value-aggregation-apply-log
   :aggregation/super-aggregation-fn sum-super-aggregation})


;; To recap, aggregations are a way of performing a operation on a unbounded stream of data. 
;; They work closely with windows and triggers to describe where and when
;; to emit this data.

;; END READ ME
