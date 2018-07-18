(defproject cletus "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.async "0.4.474"]
                 [org.clojure/tools.logging "0.4.1"]
                 [ch.qos.logback/logback-classic "1.1.8"]
                 [fullcontact/full.async "1.0.0" :exclusions [[org.clojure/clojurescript]]]
                 [spootnik/globber "0.4.1"]]
  :main ^:skip-aot cletus.core
  :target-path "target/%s"
  :profiles {:dev {:source-paths ["dev-src"]
                   :dependencies [[org.clojure/data.json "0.2.6"]]}})
