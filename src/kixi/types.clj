(ns kixi.types
  (:require [clojure.spec :as s]
            [com.gfredericks.schpec :as sh]
            [clojure.spec.gen :as gen]
            [clojure.test.check.generators :as tgen]))


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
