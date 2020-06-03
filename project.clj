(defproject parlamento "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "1.0.0"]
                 [alandipert/enduro "1.2.0"]
                 [clj-time "0.15.2"]
                 [com.taoensso/timbre "4.10.0"]
                 [diehard "0.9.4"]
                 [etaoin "0.3.6"]
                 [manifold "0.1.8"]
                 [dk.ative/docjure "1.13.0"]]
  :repl-options {:init-ns parlamento.core})
