(ns parlamento.core
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [alandipert.enduro :as pers]
            [clj-time.core :as t]
            [clj-time.coerce :refer [to-local-date]]
            [clj-time.periodic :refer [periodic-seq]]
            [diehard.core :as dh]
            [etaoin.api :as e]
            [manifold.deferred :as d]
            [manifold.stream :as st]
            [taoensso.timbre :as log]))

(defonce headless? false)
(defonce drivers (atom {:pool []
                        :idle (st/stream)}))

;; multiple drivers

(defn quit-drivers!
  []
  (pmap (fn [driver]
          (when (e/running? driver)
            (e/quit driver)))
        (:pool @drivers))

  (when-let [s (:idle @drivers)]
    (st/close! s)))


(defn init-idle!
  [n]
  (let [idle (st/stream)]
    (swap! drivers assoc :idle idle)

    (doseq [i (range n)]
      (st/put! idle i))))


(defn init-drivers!
  [n]
  (quit-drivers!)
  (swap! drivers
         assoc :pool (->> (range n)
                          (pmap (fn [_]
                                  (e/chrome {:headless headless?})))
                          (into [])))

  (init-idle! n))


(defn reserve-driver!
  []
  (let [idle (:idle @drivers)]
    @(st/take! idle)))


(defn release-driver!
  [n]
  (let [idle (:idle @drivers)]
    (st/put! idle n)))

;; --- Local db initialization

(defonce db (do
              (pers/file-atom {:active {}} "db.atom")))

(def situacao-q {:css "option[value=\"1000\"]"})
(def data-q {:css "input[title=\"Data\"]"})
(def legislatura-q {:css "select[title=\"Legislatura\"] option[value=\"\"]"})
(def pesquisar-q {:css "input[value=\"Pesquisar\"]"})
(def carregando-q {:css "span.Loading"})
(def link-biografia-q {:css "a[href^=\"/DeputadoGP/Paginas/Biografia\"]"})
(def proxima-pagina-q {:css ".pager span span + a"})
(def cookie-close-q {:id "cookieClose"})

(defn get-bids
  [driver]
  (->> (e/query-all driver link-biografia-q)
       (mapv #(-> (e/get-element-attr-el driver % "href")
                  (string/split #"=")
                  last
                  Integer/parseInt))))


(defn wait-loaded
  [driver]
  (doto driver
    (e/wait-visible carregando-q)
    (e/wait-invisible carregando-q)))


(defn get-active-in-date
  [driver date]

  (doto driver
    (e/go "https://www.parlamento.pt/DeputadoGP/Paginas/Deputados.aspx?more=1")

    (e/wait-visible legislatura-q)
    (e/click legislatura-q)

    #(when (e/visible? % cookie-close-q)
       (e/click % cookie-close-q))

    (wait-loaded)

    (e/wait-visible data-q)
    (e/clear data-q)
    (e/fill data-q date)

    (e/wait-visible situacao-q)
    (e/click situacao-q)

    (e/wait-visible pesquisar-q)
    (e/click pesquisar-q)

    (wait-loaded))

  (loop [acc []]
    (let [new-acc (into acc (get-bids driver))]
      (if (e/exists? driver proxima-pagina-q)
        (do
          (e/click driver proxima-pagina-q)
          (wait-loaded driver)
          (recur new-acc))
        new-acc))))


(defn date-range [date1 date2]
  (let [ldate1 (to-local-date date1)
        ldate2 (to-local-date date2)
        a-day  (t/days 1)]
    (->> (periodic-seq ldate1 a-day)
         (take-while #(t/before? % (t/plus ldate2 a-day)))
         (mapv str))))


(defn scraped?
  [date]
  (contains? (:active @db) date))

(defn save-active-in-date!
  [driver date]
  (pers/swap! db
              update :active
              assoc date (get-active-in-date driver date)))


(defn scrape-active-multi!
  [date1 date2]
  (let [n          (count (:pool @drivers))
        dates      (st/stream)
        gen-worker #(future
                      (loop []
                        (let [date @(-> (st/take! dates)
                                        (d/timeout! 1000 nil))]

                          (if date
                            (if-not (scraped? date)
                              (let [n-driver (reserve-driver!)
                                    driver   (-> @drivers :pool (get n-driver))]
                                (log/info "Worker" % "on" date)
                                (dh/with-retry {:retry-on          Exception
                                                :on-failed-attempt (fn [_ e]
                                                                     ; (log/error e)
                                                                     (log/error "Scraping" date "failed on worker" %))}
                                               (save-active-in-date! driver date))
                                (release-driver! n-driver)
                                (recur))
                              (do
                                (log/info "Worker" % "skipped" date)
                                (recur)))
                            (log/info "Worker" % "leaving.")))))]

    (st/put-all! dates (date-range date1 date2))

    {:missing-dates dates
     :workers       (mapv gen-worker (range n))}))


(defn cancel-scrape!
  [{:keys [workers]}]
  (mapv future-cancel workers)
  (init-idle! (count (:pool @drivers))))


(comment
  (init-drivers! 16)
  (def scraper (scrape-active-multi! "1999-10-25" "2019-10-24"))
  (cancel-scrape! scraper)
  )