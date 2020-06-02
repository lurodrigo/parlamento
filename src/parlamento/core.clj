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

;; -- Driver Management

(defonce headless? false)
(defonce drivers (atom {:pool []
                        :idle (st/stream)}))

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

;; -- DB Management

(defonce db (do
              (pers/file-atom {:active {}
                               :bio    {}} "db.atom")))

(defn bids-on-db
  []
  (->> (:active @db)
       (mapv (fn [[_ v]] (set v)))
       (apply set/union)))

(defn count-days-on-db
  []
  (count (:active @db)))

;; -- Página de Pesquisa

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


(defn get-active
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

;; -- Biografia

(defn column-wise->row-wise
  "Converts a map of sequences (column-wise) to a sequence of maps (row-wise)."
  [m]
  (apply mapv
         (fn [& args]
           (zipmap (keys m) args))
         (vals m)))

(def bio-titulo-q {:css ".TextoRegular-Titulo"})
(def linha-legislatura-q {:css ".Repeater-Cell-First"})
(defn linha-da-coluna
  [n]
  {:css (format ".Repeater-Cell:nth-child(%d)" n)})

(defn column-links
  [driver q]
  (->> (e/query-all driver q)
       (mapv #(try
                (let [el (e/child driver % {:css "a"})]
                  (e/get-element-attr-el driver el "href"))
                (catch Exception _ nil)))))

(defn column
  [driver q]
  (->> (e/query-all driver q)
       (mapv (partial e/get-element-text-el driver))))

(defn get-bio
  [driver bid]
  (doto driver
    (e/go (str "https://www.parlamento.pt/DeputadoGP/Paginas/Biografia.aspx?BID=" bid))
    (e/wait-visible bio-titulo-q))

  (let [title-info (zipmap ["Nome" "Partido"]
                           (-> (e/get-element-text driver {:css ".Titulo-Direita"})
                               (string/split #"\n")))
        details    (->> (e/query-all driver {:css ".TextoRegular-Titulo"})
                        (mapv (partial e/get-element-text-el driver))
                        (mapcat #(let [[title & lines] (string/split % #"\n")]
                                   (when-not (empty? lines)
                                     [[title (vec lines)]])))
                        (into {}))
        tabela     (column-wise->row-wise {"Legislatura"
                                           (rest (column driver linha-legislatura-q))
                                           "Registro de interesses"
                                           (rest (column-links driver (linha-da-coluna 4)))
                                           "Círculo Eleitoral"
                                           (rest (column driver (linha-da-coluna 5)))
                                           "Grupo Parlamentar/Partido"
                                           (rest (column driver (linha-da-coluna 6)))})]
    (merge title-info
           {:complementar details}
           {:tabela tabela})))


; -- Scrapers


(defn gen-worker-factory
  [job-stream scrape-name already-scraped-pred scrape-and-save!]
  (fn [worker-id]
    (future
      (loop []
        (let [job @(-> (st/take! job-stream)
                       (d/timeout! 1000 nil))]

          (if job
            (if-not (already-scraped-pred job)
              (let [n-driver (reserve-driver!)
                    driver   (-> @drivers :pool (get n-driver))]
                (log/info (format "[%s] Worker %s on job %s." scrape-name (str worker-id) (str job)))
                (dh/with-retry {:retry-on          Exception
                                :on-failed-attempt (fn [_ e]
                                                     ; (log/error e)
                                                     (log/error (format "[%s] Worker %s: job %s failed." scrape-name (str worker-id) (str job))))}
                               (scrape-and-save! driver job))
                (release-driver! n-driver)
                (recur))

              (do
                (log/info (format "[%s] Worker %s skipped job %s." scrape-name (str worker-id) (str job)))
                (recur)))

            (log/info (format "[%s] Worker %s leaving." scrape-name (str worker-id)))))))))


(defn date-scraped?
  [date]
  (contains? (:active @db) date))


(defn save-active!
  [driver date]
  (pers/swap! db
              update :active
              assoc date (get-active driver date)))


(defn date-range [date1 date2]
  (let [ldate1 (to-local-date date1)
        ldate2 (to-local-date date2)
        a-day  (t/days 1)]
    (->> (periodic-seq ldate1 a-day)
         (take-while #(t/before? % (t/plus ldate2 a-day)))
         (mapv str))))


(defn scrape-active!
  [date1 date2]
  (let [n     (count (:pool @drivers))
        dates (st/stream)]

    (st/put-all! dates (date-range date1 date2))

    {:missing dates
     :workers (mapv (gen-worker-factory dates "Active" date-scraped? save-active!) (range n))}))


(defn bio-scraped?
  [bid]
  (contains? (:bio @db) bid))


(defn save-bio!
  [driver bid]
  (pers/swap! db
              update :bio
              assoc bid (get-bio driver bid)))


(defn scrape-bios!
  []
  (let [n    (count (:pool @drivers))
        bids (st/stream)]

    (st/put-all! bids (bids-on-db))

    {:missing bids
     :workers (mapv (gen-worker-factory bids "Bio" bio-scraped? save-bio!) (range n))}))


(defn cancel-scrape!
  [{:keys [workers]}]
  (mapv future-cancel workers)
  (init-idle! (count (:pool @drivers))))


(comment
  (init-drivers! 16)

  (def scraper (scrape-active! "1999-10-25" "2019-10-24"))

  (def scraper (scrape-bios!))

  (cancel-scrape! scraper)
  )