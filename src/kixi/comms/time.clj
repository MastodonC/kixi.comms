(ns kixi.comms.time
  (:require [clj-time.core   :as t]
            [clj-time.format :as tf]))

(defn timestamp?
  [s]
  (tf/parse
   (tf/formatters :basic-date-time)
   s))

(defn timestamp
  []
  (tf/unparse
   (tf/formatters :basic-date-time)
   (t/now)))
