(ns event-bus-persistence)

(defprotocol PersistenceStore
  (persist-message! [store envelope])
  (fetch-pending! [store limit])
  (record-attempt! [store message-id])
  (mark-sent! [store message-id])
  (mark-failed! [store message-id])
  (mark-received! [store message-id])
  (was-received? [store message-id]))
