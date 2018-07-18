(ns cletus.import
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]))

(defn -main
  [& args]
  (let [spec (-> (io/reader "https://raw.github.com/antirez/redis-doc/master/commands.json")
                 (json/read))]))