(ns kixi.comms)

(defprotocol Communications
  (send-event! [this event version payload])
  (send-command! [this command version payload])
  (attach-event-handler! [this event version handler])
  (attach-command-handler! [this event version handler]))
