(ns parlamento.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as string]
            [alandipert.enduro :as pers]
            [clj-time.core :as t]
            [clj-time.coerce :refer [to-date to-local-date to-date-time]]
            [clj-time.periodic :refer [periodic-seq]]
            [diehard.core :as dh]
            [dk.ative.docjure.spreadsheet :as x]
            [etaoin.api :as e]
            [manifold.deferred :as d]
            [manifold.stream :as st]
            [taoensso.timbre :as log]))

; <editor-fold desc="Driver Management">

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

; </editor-fold>

; <editor-fold desc="DB Management">

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
; </editor-fold>

; <editor-fold desc="Página de pesquisa">

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

; </editor-fold>

; <editor-fold desc="Biografia">

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
; </editor-fold>

; <editor-fold desc="Scrapers">

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

; </editor-fold>

; <editor-fold desc="Export">

(defn within?
  [[date1 date2] date]
  (t/within? (t/interval (to-date-time date1)
                         (t/plus (to-date-time date2)
                                 (t/days 1)))
             (to-date-time date)))

(def ^:const legislaturas
  {"XIII" ["2015-10-23" "2019-10-24"]
   "XII"  ["2011-06-20" "2015-10-22"]
   "XI"   ["2009-10-15" "2011-06-19"]
   "X"    ["2005-03-10" "2009-10-14"]
   "IX"   ["2002-04-05" "2005-03-09"]
   "VIII"  ["1999-10-25" "2002-04-04"]})


(defn legislatura
  [date]
  (loop [legs legislaturas]
    (when-not (empty? legs)
      (let [[symbol interval] (first legs)]
        (if (within? interval date)
          symbol
          (recur (rest legs)))))))


(def ^:const col-order-active
  {
   ;"Condecorações e Louvores" 8
   ;"Títulos académicos e científicos" 9
   "Círculo Eleitoral"         14                           ;
   ;"Falecido/a" 11
   ;"Obras Publicadas" 10
   "Grupo Parlamentar/Partido" 15                           ;
   "Legislatura"               13                           ;
   "Data Ativo"                12                           ;
   ;"Data de nascimento" 2
   "Nome resumido"             0                            ;
   ;"Nome completo" 1
   ;"Habilitações literárias" 4
   ;"Profissão" 3
   ;"Cargos exercidos" 6
   ;"Cargos que desempenha" 5
   ;"Comissões Parlamentares a que pertence" 7
   })


(defn party-at-date
  [date entry]
  (let [lines (string/split-lines entry)]
    (if (= 1 (count lines))
      entry
      (let [date-split (re-find #"\d{4}-\d{2}-\d{2}" (lines 1))]
        (case (compare date date-split)
          -1
          (lines 2)

          (lines 0))))))

(defn build-entry-active
  [db-content date bid]
  (let [bio  (get-in db-content [:bio bid])
        legs (legislatura date)]

    (merge {"Data Ativo"    date
            "Nome resumido" (get bio "Nome")
            "Legislatura"   legs}

           (-> (select-keys (->> (:tabela bio)
                                 (filter #(= (get % "Legislatura") legs))
                                 first)
                            ["Círculo Eleitoral" "Grupo Parlamentar/Partido"])
               (update "Grupo Parlamentar/Partido"
                       (partial party-at-date date))))))


(defn build-entry-bio
  [db-content bid]
  (let [bio (get-in db-content [:bio bid])]
    (merge {"Nome resumido" (get bio "Nome")}
           (->> (:complementar bio)
                (mapv (fn [[k v]]
                        [k (string/join "\n" v)]))
                (into {})))))


(def ^:const col-order-bio
  {
   "Condecorações e Louvores"               8
   "Títulos académicos e científicos"       9
   ;"Círculo Eleitoral"                      14
   "Falecido/a"                             11
   "Obras Publicadas"                       10
   ;"Grupo Parlamentar/Partido"              15
   ;"Legislatura"                            13
   ;"Data Ativo"                             12
   "Data de nascimento"                     2
   "Nome resumido"                          0
   "Nome completo"                          1
   "Habilitações literárias"                4
   "Profissão"                              3
   "Cargos exercidos"                       6
   "Cargos que desempenha"                  5
   "Comissões Parlamentares a que pertence" 7
   })


(defn ->col-order-vec
  [col-order-map]
  (->> (set/map-invert col-order-map)
       (into (sorted-map))
       vals
       vec))


(defn row-builder
  [col-order-vec]
  (let [cols-in-order (mapv (fn [col] #(get % col "")) col-order-vec)]
    (apply juxt cols-in-order)))


(defn build-active-table
  [interval]
  (let [db-content @db
        col-order-vec (->col-order-vec col-order-active)]
    (->> (:active db-content)
         (filter (fn [[date _]]
                   (within? interval date)))
         (mapcat (fn [[date bids]]
                   (map (partial build-entry-active db-content date) bids)))
         (map (row-builder col-order-vec))
         (into [col-order-vec]))))


(defn export-active!
  ([]
   (export-active! "target/%s Legislatura.csv"))
  ([filename-format]
   (for [[leg interval] legislaturas]
     (->> (build-active-table interval)
          (csv/write-csv (io/writer (format filename-format leg)))))))


(defn build-bio-table
  []
  (let [db-content @db
        col-order-vec (->col-order-vec col-order-bio)]
    (->> (keys (:bio db-content))
         (map (partial build-entry-bio db-content))
         (map (row-builder col-order-vec))
         (into [col-order-vec]))))


(defn export-bio!
  ([]
   (export-bio! "target/Biografias.csv"))
  ([filename]
   (->> (build-bio-table)
        (csv/write-csv (io/writer filename)))))

; </editor-fold>

(comment
  (init-drivers! 16)

  (def scraper (scrape-active! "1999-10-25" "2019-10-24"))

  (def scraper (scrape-bios!))

  (cancel-scrape! scraper)
  )