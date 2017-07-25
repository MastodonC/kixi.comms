(ns kixi.types
  (:require [clojure.spec :as s]
            [com.gfredericks.schpec :as sh]
            [clojure.spec.gen :as gen]
            [clojure.test.check.generators :as tgen]
            [clj-time.core :as t]
            [clj-time.format :as tf]))

(defn -regex?
  [rs]
  (fn [x]
    (if (and (string? x) (re-find rs x)) 
      x 
      :clojure.spec/invalid)))

(def uuid?
  (-regex? #"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))

(def uuid 
  (s/with-gen 
    (s/conformer uuid?)
    #(tgen/no-shrink (gen/fmap str (gen/uuid)))))


(def format :basic-date-time)

(def formatter
  (tf/formatters format))

(def time-parser   
  (partial tf/parse formatter))

(defn timestamp?
  [x]
  (if (instance? org.joda.time.DateTime x)
    x
    (try
      (if (string? x)
        (time-parser x)
        :clojure.spec/invalid)
      (catch IllegalArgumentException e
        :clojure.spec/invalid))))

(def timestamp
  (s/with-gen
    (s/conformer timestamp?)
    #(gen/return (t/now))))
