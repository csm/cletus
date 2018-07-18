(ns cletus.handler
  (:require [clojure.core.async :as async]
            [cletus.resp :as resp]
            [clojure.string :as string])
  (:import [java.util.concurrent TimeUnit]))

(def state (atom {}))

(defn success-chan
  [v]
  (doto (async/chan 1)
    (async/put! v)
    (async/close!)))

(defn- to-bytes
  [v]
  (cond (instance? (type (byte-array 0)) v) v
        (string? v) (.getBytes v)))

(defmulti handle-command (fn [command & args] command))

(defmethod handle-command :COMMAND
  [& args]
  (success-chan [])) ; TODO

(defmethod handle-command :GET
  [_ key]
  (async/thread
    (if-let [v (get @state (resp/->Bytes (to-bytes key)))]
      (if (instance? cletus.resp.Bytes v)
        (.-bytes v)
        (resp/->RedisError "WRONGTYPE Operation against a key holding the wrong kind of value"))
      :nil)))

(defmethod handle-command :GETBIT
  [_ key index]
  (async/thread
    (if-let [v (get @state (resp/->Bytes (to-bytes key)))]
      (if (instance? cletus.resp.Bytes v)
        (let [b (if (< (int (/ index 8)) (alength (.-bytes v)))
                  (aget (:bytes v) (int (/ index 8)))
                  0)]
          (bit-and (bit-shift-right b (- 8 (mod index 8))) 0xFF)
          (resp/->RedisError "WRONGTYPE Operation against a key holding the wrong kind of value")))
      0)))

(defmethod handle-command :SET
  [_ key value & args]
  (async/thread
    (swap! state assoc (resp/->Bytes (to-bytes key)) (resp/->Bytes (to-bytes value)))
    "OK"))

(defmethod handle-command :TIME
  [& args]
  (let [t (System/currentTimeMillis)]
    (success-chan [(.getBytes (format "%d" (long (/ t 1000))))
                   (.getBytes (format "%d" (.convert TimeUnit/MICROSECONDS (mod t 1000) TimeUnit/MILLISECONDS)))])))

(defmethod handle-command :default
  [& args]
  (success-chan (resp/->RedisError "ERR unknown command")))

(defn default-handle-command
  [command]
  (apply handle-command command))