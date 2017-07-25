(ns kixi.data-types
  (:require [clojure.spec :as s]
            [com.gfredericks.schpec :as sh]
            [kixi.types :as t]))

(sh/alias 'user 'kixi.user)

(s/def ::user/id t/uuid)
(s/def ::user/groups (s/coll-of t/uuid))

(s/def :kixi/user
  (s/keys :req [::user/id
                ::user/groups]))

(sh/alias 'command 'kixi.command)

(s/def ::command/id t/uuid)
(s/def ::command/created-at t/timestamp)

(sh/alias 'event 'kixi.event)
(s/def ::event/created-at t/timestamp)

(sh/alias 'msg 'kixi.message)

(s/def ::msg/type
  #{:command :event})

