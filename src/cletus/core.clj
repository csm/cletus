(ns cletus.core
  (:require [cletus.handler :refer [default-handle-command]]
            [cletus.resp :as resp]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [full.async :as fasync])
  (:import [java.io InputStream IOException]
           [java.net ServerSocket InetAddress Socket]))

(defn instream->chan
  ([stream] (instream->chan stream true))
  ([^InputStream stream propagate-exceptions?]
   (let [chan (async/chan 1024)]
     (async/thread
       (loop []
         (try)
         (let [b (try (.read stream)
                      (catch IOException e e))]
           (cond
             (instance? Exception b)
             (do
               (when propagate-exceptions?
                 (async/put! chan b))
               (async/close! chan))

             (= b -1)
             (async/close! chan)

             :else
             (if (async/>!! chan (.byteValue b))
               (recur)
               (.close stream))))))
     chan)))

(defn- redis-handler
  [command-handler]
  (fn
    [^Socket socket]
    (let [bytes-in (instream->chan (.getInputStream socket))
          commands (async/chan)
          out-stream (.getOutputStream socket)]
      ; Turn sequence of bytes into commands
      (resp/parse-resp bytes-in commands)
      ; send off commands to the handler
      (async/go-loop []
        (if-let [command (fasync/<? commands)]
          (do
            (log/debug "handling command" command)
            (if-let [reply (async/<! (command-handler command))]
              (do
                (log/debug "got reply" reply)
                (let [put (fasync/thread-try
                            (let [encoded (resp/encode reply)]
                              (log/debug "encoded reply" (pr-str (String. encoded)))
                              (.write out-stream encoded))
                            true)
                      result (async/<! put)]
                  (do
                    (log/debug "got result" result)
                    (cond (instance? Exception result)
                          (async/close! commands)

                          (true? result)
                          (recur)

                          :else
                          (async/close! commands)))))
              (async/close! commands)))
          (async/close! commands))))))

(defn start-server
  [{:keys [command-handler port bind-address backlog]
    :or   {command-handler default-handle-command
           port            6379
           bind-address    (InetAddress/getLocalHost)
           backlog         50}
    :as   opts}]
  (let [server (ServerSocket. port backlog bind-address)
        handler (redis-handler command-handler)]
    (async/thread
      (loop []
        (let [socket (.accept server)]
          (handler socket)
          (recur))))
    server))
