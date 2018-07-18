(ns cletus.resp
  (:require [clojure.core.async :as async]
            [full.async :as fasync]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import [java.io PushbackReader]))

(deftype Bytes [bytes]
  Comparable
  (compareTo [_ that]
    (let [res (reduce (fn [r [b1 b2]]
                        (if (zero? r)
                          (cond
                            (< (bit-and b1 0xFF) (bit-and b2 0xFF)) -1
                            (> (bit-and b1 0xFF) (bit-and b2 0xFF)) 1
                            :else 0)
                          r))
                      0
                      (map vector bytes (:bytes that)))]
      (if (zero? res)
        (cond (< (alength bytes) (alength (:bytes that))) -1
              (> (alength bytes) (alength (:bytes that))) 1
              :else 0)
        res)))

  Object
  (hashCode [_]
    (reduce #(unchecked-add (unchecked-multiply-int 31 %1) (bit-and %2 0xFF)) (int 0) bytes))

  (equals [_ that]
    (and (instance? Bytes that)
         (= (alength bytes) (alength (.-bytes that)))
         (every? #(= (first %) (first %)) (map vector bytes (.-bytes that))))))

(defn read-length
  [ch]
  (fasync/go-try
    (loop [result 0
           sign 1
           prev nil
           n 0]
      (when-let [b (async/<! ch)]
        (cond (<= (int \0) b (int \9))
              (recur (+ (* result 10) (- b (int \0))) sign b (inc n))

              (and (= \- b) (= 0 n))
              (recur result -1 b (inc n))

              (and (= \return (char b)) (pos? n) (<= (int \0) prev (int \9)))
              (recur result sign b (inc n))

              (and (= \newline (char b)) (= \return (char prev)) (pos? n))
              (* sign result)

              :else
              (throw (ex-info "illegal character, expecting integer" {:char b :pos n})))))))

(declare read-value)

(defmacro consume-crlf
  [ch]
  `(do
     (let [b# (async/<! ~ch)]
       (log/debug "consumed" b#)
       (when (not= \return (char b#))
         (throw (ex-info (str "parse error, expected \return, got " (some-> b# char pr-str)) {}))))
     (let [b# (async/<! ~ch)]
       (log/debug "consumed" b#)
       (when (not= \newline (char b#))
         (throw (ex-info (str "parse error, expected \newline, got " (some-> b# char pr-str)) {}))))))

(defn read-array
  [ch]
  (fasync/go-try
    (let [len (fasync/<? (read-length ch))]
      (cond
        (= len -1) nil

        (or (zero? len) (pos? len))
        (loop [count len result []]
          (if (pos? count)
            (recur (dec count) (conj result (fasync/<? (read-value ch))))
            result))

        :else (throw (ex-info "invalid array length" {:length len}))))))

(defn read-bulk-string
  [ch]
  (fasync/go-try
    (let [len (fasync/<? (read-length ch))]
      (log/debug "bulk string length:" len)
      (cond (= len -1)
            (do
              (consume-crlf ch)
              nil)

            (or (zero? len) (pos? len))
            (loop [i 0 result (byte-array len)]
              (if (< i len)
                (if-let [b (async/<! ch)]
                  (do
                    (log/debug "consumed byte" b)
                    (aset-byte result i b)
                    (recur (inc i) result))
                  (throw (ex-info "short read" {:expected len :got i})))
                (do
                  (consume-crlf ch)
                  result)))

            :else
            (throw (ex-info "invalid bulk string length" {:length len}))))))

(defn read-simple-string
  [ch]
  (fasync/go-try
    (loop [prev nil
           value ""]
      (if-let [b (async/<! ch)]
        (let [c (char (bit-and (int b) 0xFF))]
          (cond
            (<= (int \space) b (int \~))
            (recur c (str value c))

            (= \return c)
            (recur c value)

            (and (= \newline c) (= \return prev))
            value

            :else
            (throw (ex-info "invalid character in simple string" {:char c :prev prev}))))))))

(defrecord RedisError [message])

(defn read-error
  [ch]
  (fasync/go-try
    (let [msg (fasync/<? (read-simple-string ch))]
      (->RedisError msg))))

(defn read-value
  [ch]
  (fasync/go-try
    (when-let [b (async/<! ch)]
      (log/debug "dispatch value" (pr-str (char b)))
      (let [result (case (char b)
                     \* (fasync/<? (read-array ch))
                     \$ (fasync/<? (read-bulk-string ch))
                     \+ (fasync/<? (read-simple-string ch))
                     \- (fasync/<? (read-error ch))
                     \: (fasync/<? (read-length ch)))]
        (log/debug "read value" result)
        result))))

(def spec (delay (edn/read (PushbackReader. (io/reader (io/resource "spec.edn"))))))

(defn ->string
  [v]
  (cond (string? v) v
        (instance? (type (byte-array 0)) v) (String. ^"[B" v "UTF-8")))

(defn safe-keyword
  [v]
  (keyword (string/replace v #"[\s]" "_")))

(defn parse-arg
  [type value enums]
  (case type
    "key" value
    "string" (String. value "UTF-8")
    "integer" (long value)
    "pattern" (String. value "UTF-8")
    "enum" ((set enums) (String. value "UTF-8"))))

(defn parse-arguments
  [arg-specs args]
  (cond
    (and (empty? arg-specs) (not-empty args))
    (throw (ex-info "extra arguments found" {:args args}))

    (and (empty? args) (or (empty? arg-specs)
                           (-> arg-specs first :optional)
                           (-> arg-specs first :multiple)))
    []

    :else
    (let [{:keys [command enum name type optional multiple]} (first arg-specs)]
      (if (some? command)
        (let [[cmd & args] args]
          (if (= command cmd)
            (let [type (if (seq type) type [type])]
              (concat [(concat [cmd] (map #(parse-arg %1 %2 nil) type (take (count type) args)))]
                      (parse-arguments (if multiple arg-specs (rest arg-specs)) (drop (count type) args))))
            (if optional
              (parse-arguments (rest arg-specs) args)
              (throw (ex-info (str "command " command " is required") {})))))
        (cond
          (string? type) (concat [(parse-arg type (first args) enum)]
                                 (parse-arguments (if multiple arg-specs (rest arg-specs)) (rest args)))
          (seq type) (concat (map #(parse-arg %1 %2 nil) type (take (count type) args))
                             (parse-arguments (if multiple arg-specs (rest arg-specs)) (drop (count type) args))))))))



(defn parse-command
  [cmd]
  (log/debug "parsing command" cmd)
  (if-let [command (some->> (first cmd) (->string) (string/upper-case) (safe-keyword))]
    (if-let [cmd-spec (get @spec command)]
      (if-let [args (get cmd-spec :arguments)]
        (concat [command] (parse-arguments args (rest cmd)))
        [command])
      [])
    []))

(defn parse-resp
  "Parse bytes received from bytes-chan into Redis commands,
  sending each command to commands-chan."
  [bytes-chan commands-chan]
  (fasync/go-try
    (loop []
      (when-let [v (async/<! (read-value bytes-chan))]
        (let [cmd (try (parse-command v)
                       (catch Exception e e))]
          (when (async/>! commands-chan cmd)
            (recur)))))))

(defn- concat-bytes
  [arg & args]
  (if (empty? args)
    arg
    (let [b (byte-array (+ (count arg) (count (first args))))]
      (System/arraycopy arg 0 b 0 (count arg))
      (System/arraycopy (first args) 0 b (count arg) (count (first args)))
      (recur b (rest args)))))

(defn encode
  [v]
  (cond
    (or (nil? v) (= v :nil))
    (.getBytes "$-1\r\n")

    (and (string? v) (not (string/includes? v "\r\n")))
    (concat-bytes (byte-array [(byte \+)]) (.getBytes v) (.getBytes "\r\n"))

    (instance? RedisError v)
    (concat-bytes (byte-array [(byte \-)]) (.getBytes (:message v)) (.getBytes "\r\n"))

    (integer? v)
    (concat-bytes (byte-array [(byte \:)]) (.getBytes (format "%d" v)) (.getBytes "\r\n"))

    (sequential? v)
    (apply concat-bytes (byte-array [(byte \*)]) (.getBytes (format "%d" (count v))) (.getBytes "\r\n") (map encode v))

    (instance? (type (byte-array 0)) v)
    (concat-bytes (byte-array [(byte \$)]) (.getBytes (format "%d" (alength v))) (.getBytes "\r\n") v (.getBytes "\r\n"))

    (instance? Throwable v)
    (encode (->RedisError (str "ERR " (.getMessage v))))))