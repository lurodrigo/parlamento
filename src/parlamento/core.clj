(ns parlamento.core
  (:require [clojure.set :as set]
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
              (pers/file-atom {:active             {}
                               :efetivo-temporario {}
                               :efetivo-definitivo {}
                               :bio                {}
                               :int                {"XI" {}, "XII" {}, "XIII" {}}} "db.atom")))

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

(def ativo-q {:css "option[value=\"1000\"]"})
(def efetivo-definitivo-q {:css "select[title=\"Situação do Deputado\"] option[value=\"2\"]"})
(def efetivo-temporario-q {:css "select[title=\"Situação do Deputado\"] option[value=\"3\"]"})
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


(defn click-cookies!
  [driver]
  (doto driver
    (e/go "https://www.parlamento.pt/DeputadoGP/Paginas/Deputados.aspx?more=1")
    (e/wait-visible cookie-close-q)
    (e/click cookie-close-q)))


(defn get-deputies-situation-at-date
  [driver situacao-q date]

  (doto driver
    (e/go "https://www.parlamento.pt/DeputadoGP/Paginas/Deputados.aspx?more=1")

    (e/wait-visible legislatura-q)
    (e/click legislatura-q)

    ;(e/click cookie-close-q)

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

; <editor-fold desc="Registro de Interesses">

(defn in-id-ending-with
  ([id]
   {:css (format "[id$=\"%s\"]" id)})
  ([id rest]
   {:css (format "[id$=\"%s\"] %s" id rest)}))

; <editor-fold desc="Versão 1">

(defn titulo-secao-v1
  [driver quasi-id]
  (if (= quasi-id "pnlOutrasSituacoes")
    (e/get-element-text driver (in-id-ending-with quasi-id ".TitulosBio.row"))
    (e/get-element-text driver (in-id-ending-with quasi-id ".Titulo-Cinzento.row"))))

(defn emptify
  [s]
  (if (string/starts-with? s "__")
    ""
    s))

(defn conteudo-secao-v1
  [driver quasi-id {:keys [type count]}]
  (case type
    :text
    (emptify (e/get-element-text driver (in-id-ending-with quasi-id ".TextoRegular")))

    (let [ks (mapv (partial e/get-element-text-el driver)
                   (e/query-all driver (in-id-ending-with quasi-id
                                                          ".TextoRegular-Titulo")))
          vs (mapv (comp emptify (partial e/get-element-text-el driver))
                   (e/query-all driver (in-id-ending-with quasi-id
                                                          ".TextoRegular")))]
      (case type
        :map
        (zipmap ks vs)

        :many-maps
        (->> (interleave ks vs)
             (partition count)
             (mapv (partial apply hash-map)))))))


(defn map-secao-v1
  ([driver quasi-id]
   (map-secao-v1 driver quasi-id {:type :map}))
  ([driver quasi-id args]
   (try
     {(titulo-secao-v1 driver quasi-id)
      (conteudo-secao-v1 driver quasi-id args)}
     (catch Exception _ {}))))


(def r1-url "https://www.parlamento.pt/DeputadoGP/Paginas/RegistoInteresses.aspx?BID=3")

(defn registro-interesses-v1
  [driver url]
  (e/go driver url)
  (e/wait-visible driver {:css ".Titulo-Cinzento"})

  (->> [["pnlIdentificacaoDeclarante"]
        ["pnlCargoExerce"]
        ["pnlActividades" {:type :text}]
        ["pnlCargosSociais" {:type :many-maps :count 8}]
        ["pnlApoiosBeneficios" {:type :text}]
        ["pnlServicosPrestados" {:type :text}]
        ["pnlSociedades" {:type :many-maps :count 8}]
        ["pnlOutrasSituacoes" {:type :text}]]
       (mapv (partial apply map-secao-v1 driver))
       (apply merge)))


; </editor-fold>

; <editor-fold desc="Versão 2">

; 208 tem cargos sociais, atividades e sociedades
(def r2-url "https://www.parlamento.pt/DeputadoGP/Paginas/XIIL_RegInteresses.aspx?BID=3&leg=XIII")

(defn titulo-secao-v2
  [driver quasi-id]
  (case quasi-id

    "pnlCargosSociais"
    "IV - Cargos sociais"

    "pnlSociedades"
    "VII - Sociedades"

    (e/get-element-text driver (in-id-ending-with quasi-id ".Titulo-Cinzento"))))


(defn conteudo-secao-v2
  [driver quasi-id {:keys [type count]}]
  (case type
    :text
    (try
      (e/get-element-text driver (in-id-ending-with quasi-id
                                                    ".TextoRegular"))
      (catch Exception _ ""))

    :map
    (try
      (let [ks (mapv (partial e/get-element-text-el driver)
                     (e/query-all driver (in-id-ending-with quasi-id
                                                            ".TextoRegular-Titulo")))
            vs (->> (e/query-all driver (in-id-ending-with quasi-id "span:not([style=\"white-space: nowrap\"])"))
                    (mapv (partial e/get-element-text-el driver)))]
        (zipmap ks vs))
      (catch Exception _ {}))

    :many-maps
    (try
      (let [ks (mapv (partial e/get-element-text-el driver)
                     (e/query-all driver (in-id-ending-with quasi-id
                                                            ".TextoRegular-Titulo")))
            vs (->> (e/query-all driver (in-id-ending-with quasi-id "span:not([style=\"white-space: nowrap\"])"))
                    (mapv (partial e/get-element-text-el driver)))]
        (->> (interleave ks vs)
             (partition count)
             (mapv (partial apply hash-map))))
      (catch Exception _ []))))


(defn map-secao-v2
  ([driver quasi-id]
   (map-secao-v2 driver quasi-id {:type :map}))
  ([driver quasi-id args]
   (try
     {(titulo-secao-v2 driver quasi-id)
      (conteudo-secao-v2 driver quasi-id args)}
     (catch Exception _ {}))))


(defn registro-interesses-v2
  [driver url]
  (e/go driver url)
  (e/wait-visible driver {:css ".Titulo-Cinzento"})

  (->> [["pnlIdentificacaoDeclarante"]
        ["pnlCargoExerce"]
        ["pnlActividades" {:type :many-maps :count 8}]
        ["pnlCargosSociais" {:type :many-maps :count 12}]
        ["pnlApoiosBeneficios" {:type :text}]
        ["pnlServicosPrestados" {:type :text}]
        ["pnlSociedades" {:type :many-maps :count 8}]
        ["pnlOutrasSituacoes" {:type :text}]]
       (mapv (partial apply map-secao-v2 driver))
       (apply merge)))

; </editor-fold>

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

; <editor-fold desc="Basic Helpers">

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
   "VIII" ["1999-10-25" "2002-04-04"]})


(def ^:const sessoes-legislativas
  {["1999-10-25" "2000-09-14"] 1
   ["2000-09-15" "2001-09-14"] 2
   ["2001-09-15" "2002-04-04"] 3
   ["2002-04-05" "2003-09-14"] 1
   ["2003-09-15" "2004-09-14"] 2
   ["2004-09-15" "2005-03-09"] 3
   ["2005-03-10" "2006-09-14"] 1
   ["2006-09-15" "2007-09-14"] 2
   ["2007-09-15" "2008-09-14"] 3
   ["2008-09-15" "2009-10-14"] 4
   ["2009-10-15" "2010-09-14"] 1
   ["2010-09-15" "2011-06-19"] 2
   ["2011-06-20" "2012-09-14"] 1
   ["2012-09-15" "2013-09-14"] 2
   ["2013-09-15" "2014-09-14"] 3
   ["2014-09-15" "2015-10-22"] 4
   ["2015-10-23" "2016-09-14"] 1
   ["2016-09-15" "2017-09-14"] 2
   ["2017-09-15" "2018-09-14"] 3
   ["2018-09-15" "2019-10-24"] 4})


(defn legislatura*
  [date]
  (loop [legs legislaturas]
    (when-not (empty? legs)
      (let [[symbol interval] (first legs)]
        (if (within? interval date)
          symbol
          (recur (rest legs)))))))


(def legislatura (memoize legislatura*))


(defn sessao-legislativa*
  [date]
  (loop [sessoes sessoes-legislativas]
    (when-not (empty? sessoes)
      (let [[interval sessao] (first sessoes)]
        (if (within? interval date)
          (str sessao)
          (recur (rest sessoes)))))))


(def sessao-legislativa (memoize sessao-legislativa*))

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
                                                     (println e)
                                                     (log/error e)
                                                     (log/error (format "[%s] Worker %s: job %s failed." scrape-name (str worker-id) (str job))))}
                               (scrape-and-save! driver job))
                (release-driver! n-driver)
                (recur))

              (do
                (log/info (format "[%s] Worker %s skipped job %s." scrape-name (str worker-id) (str job)))
                (recur)))

            (log/info (format "[%s] Worker %s leaving." scrape-name (str worker-id)))))))))


(defn date-active-scraped?
  [date]
  (contains? (:active @db) date))

(defn date-efetivo-temporario-scraped?
  [date]
  (contains? (:efetivo-temporario @db) date))


(defn date-efetivo-definitivo-scraped?
  [date]
  (contains? (:efetivo-definitivo @db) date))


(defn interesse-scraped?
  [[leg bid]]
  (contains? (get-in @db [:int leg]) bid))


(defn save-active!
  [driver date]
  (pers/swap! db
              update :active
              assoc date (get-deputies-situation-at-date driver ativo-q date)))


(defn get-interesse-url
  [db-content [leg bid]]
  (try
    (let [tb    (get-in db-content [:bio bid :tabela])
          entry (filter #(= leg (get % "Legislatura")) tb)]
      (if (empty? entry)
        nil
        (get (first entry) "Registro de interesses")))
    (catch Exception _ nil)))


(defn save-interesse-v1!
  [driver [leg bid url]]
  (pers/swap! db
              update :int
              update leg
              assoc bid (registro-interesses-v1 driver url)))


(defn save-interesse-v2!
  [driver [leg bid url]]
  (pers/swap! db
              update :int
              update leg
              assoc bid (registro-interesses-v2 driver url)))


(defn save-efetivo-temporario!
  [driver date]
  (pers/swap! db
              update :efetivo-temporario
              assoc date (get-deputies-situation-at-date driver efetivo-temporario-q date)))


(defn save-efetivo-definitivo!
  [driver date]
  (pers/swap! db
              update :efetivo-definitivo
              assoc date (get-deputies-situation-at-date driver efetivo-definitivo-q date)))


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
     :workers (mapv (gen-worker-factory dates "Active" date-active-scraped? save-active!) (range n))}))


(defn scrape-efetivo-temporario!
  [date1 date2]
  (let [n     (count (:pool @drivers))
        dates (st/stream)]

    (st/put-all! dates (date-range date1 date2))

    {:missing dates
     :workers (mapv (gen-worker-factory dates "Efetivo Temporário" date-efetivo-temporario-scraped? save-efetivo-temporario!) (range n))}))


(defn scrape-efetivo-definitivo!
  [date1 date2]
  (let [n     (count (:pool @drivers))
        dates (st/stream)]

    (st/put-all! dates (date-range date1 date2))

    {:missing dates
     :workers (mapv (gen-worker-factory dates "Efetivo Definitivo" date-efetivo-definitivo-scraped? save-efetivo-definitivo!) (range n))}))


(defn com-interesse [leg]
  (let [interval   (legislaturas leg)
        db-content @db]
    (->> (:active @db)
         (filter (fn [[date _]]
                   (within? interval date)))
         (vals)
         (mapv set)
         (apply set/union)
         (filter #(get-interesse-url db-content [leg %])))))


(defn scrape-interesse-v1!
  []
  (let [n          (count (:pool @drivers))
        jobs       (st/stream)
        db-content @db]

    (st/put-all! jobs (mapv (fn [bid]
                              ["XI" bid (get-interesse-url db-content ["XI" bid])])
                            (com-interesse "XI")))

    {:missing jobs
     :workers (mapv (gen-worker-factory jobs "Interesse v1" interesse-scraped? save-interesse-v1!) (range n))}))


(defn scrape-interesse-v2!
  []
  (let [n          (count (:pool @drivers))
        jobs       (st/stream)
        db-content @db]

    (st/put-all! jobs (concat
                        (mapv (fn [bid]
                                ["XII" bid (get-interesse-url db-content ["XII" bid])])
                              (com-interesse "XII"))
                        (mapv (fn [bid]
                                ["XIII" bid (get-interesse-url db-content ["XIII" bid])])
                              (com-interesse "XIII"))))

    {:missing jobs
     :workers (mapv (gen-worker-factory jobs "Interesse v1" interesse-scraped? save-interesse-v2!) (range n))}))


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

; <editor-fold desc="Export Active">

(def ^:const col-order-active
  {
   ;"Condecorações e Louvores" 8
   ;"Títulos académicos e científicos" 9
   "Círculo Eleitoral"         14                           ;
   ;"Falecido/a" 11
   ;"Obras Publicadas" 10
   "Grupo Parlamentar/Partido" 15
   "Sessão Legislativa"        13                           ;
   "Legislatura"               12                           ;
   ;"Data Ativo"                12                           ;
   "Data início"               9
   "Data fim"                  10
   "Dias"                      11
   ;"Data de nascimento" 2
   "Situação"                  1
   "Nome resumido"             0
   "BID"                       -1                           ;
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
        (if (t/before? (to-date-time date) (to-date-time date-split))
          (lines 2)
          (lines 0))))))


(defn situacao-no-dia?
  [kw db-content date bid]
  (contains? (set (get-in db-content [kw date])) bid))


(def temporario-no-dia? (partial situacao-no-dia? :efetivo-temporario))
(def definitivo-no-dia? (partial situacao-no-dia? :efetivo-definitivo))
(def ativo-no-dia? (partial situacao-no-dia? :active))


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


(defn field
  "Generates an accessor function for a (possibly deep) field."
  [& fields]
  #(get-in % fields))


(defn contiguous?* [a b]
  (= 1
     (-> (t/interval (to-date-time a) (to-date-time b))
         (t/in-days))))


(defn contiguous? [a b]
  (and (= 1
          (-> (t/interval (to-date-time (get a "Data Ativo"))
                          (to-date-time (get b "Data Ativo")))
              (t/in-days)))
       (= (get a "Grupo Parlamentar/Partido")
          (get b "Grupo Parlamentar/Partido"))
       (= (get a "Sessão Legislativa")
          (get b "Sessão Legislativa"))
       (= (get a "Situação")
          (get b "Situação"))))


(defn partition-by-contiguity
  [l]
  (let [v (vec l)]
    (loop [acc []
           grp [(first v)]
           i   1]
      (if (= i (count v))
        (conj acc grp)
        (let [current  (v i)
              previous (v (dec i))]
          (if (contiguous? previous current)
            (recur acc (conj grp current) (inc i))
            (recur (conj acc grp) [current] (inc i))))))))


(defn summarise-contiguous
  [[fst :as items]]
  (-> (select-keys fst
                   ["BID" "Nome resumido" "Situação" "Legislatura" "Sessão Legislativa" "Círculo Eleitoral" "Grupo Parlamentar/Partido"])
      (merge {"Data início" (get fst "Data Ativo")
              "Data fim"    (get (last items) "Data Ativo")
              "Dias"        (str (count items))})))


(defn build-entry-active
  [db-content date bid]
  (let [bio    (get-in db-content [:bio bid])
        legs   (legislatura date)
        sessao (sessao-legislativa date)]

    (merge {"Data Ativo"         date
            "BID"                bid
            "Nome resumido"      (get bio "Nome")
            "Legislatura"        legs
            "Sessão Legislativa" sessao
            "Situação"           (cond
                                   (definitivo-no-dia? db-content date bid)
                                   "Efetivo Definitivo"

                                   (temporario-no-dia? db-content date bid)
                                   "Efetivo Temporário"

                                   (ativo-no-dia? db-content date bid)
                                   "Efetivo"

                                   :else
                                   "Possivelmente suplente")}

           (-> (select-keys (->> (:tabela bio)
                                 (filter #(= (get % "Legislatura") legs))
                                 first)
                            ["Círculo Eleitoral" "Grupo Parlamentar/Partido"])

               (update "Grupo Parlamentar/Partido"
                       (partial party-at-date date))))))


(defn build-active-table
  [interval]
  (let [db-content    @db
        col-order-vec (->col-order-vec col-order-active)]
    (->> (:active db-content)
         (filter (fn [[date _]]
                   (within? interval date)))
         (mapcat (fn [[date bids]]
                   (map (partial build-entry-active db-content date) bids)))
         (sort-by (juxt (field "Nome resumido") (field "Data Ativo")))
         (partition-by (field "Nome resumido"))
         (mapv partition-by-contiguity)
         (mapv #(mapv summarise-contiguous %))
         flatten
         (map (row-builder col-order-vec))
         (into [col-order-vec])
         vec)))


(defn export-active!
  ([]
   (export-active! "target/%s Legislatura.xlsx"))
  ([filename-format]
   (for [[leg interval] legislaturas]
     (->> (x/create-workbook (str leg " Legislatura") (build-active-table interval))
          (x/save-workbook-into-file! (format filename-format leg))))))

; </editor-fold>

; <editor-fold desc="Export Bio">

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
   "BID"                                    -1
   "Nome resumido"                          0
   "Nome completo"                          1
   "Habilitações literárias"                4
   "Profissão"                              3
   "Cargos exercidos"                       6
   "Cargos que desempenha"                  5
   "Comissões Parlamentares a que pertence" 7
   })


(defn build-entry-bio
  [db-content bid]
  (let [bio (get-in db-content [:bio bid])]
    (merge {"Nome resumido" (get bio "Nome")
            "BID"           bid}
           (->> (:complementar bio)
                (mapv (fn [[k v]]
                        [k (string/join "\n" v)]))
                (into {})))))


(defn build-bio-table
  []
  (let [db-content    @db
        col-order-vec (->col-order-vec col-order-bio)]
    (->> (keys (:bio db-content))
         (mapv (partial build-entry-bio db-content))
         (map (row-builder col-order-vec))
         (into [col-order-vec]))))


(defn export-bio!
  ([]
   (export-bio! "target/Biografias.xlsx"))
  ([filename]
   (->> (build-bio-table)
        (x/create-workbook "Biografia")
        (x/save-workbook-into-file! filename))))

; </editor-fold>

; <editor-fold desc="Export Interesses v1">

(def col-order-interesse-main-v1
  {"BID"                                                        0
   "V - Apoios ou Benefícios"                                   9,
   "II - Cargo que exerce - Ano de"                             7,
   "II - Cargo que exerce - Cargo"                              6,
   "VI - Serviços Prestados"                                    10,
   "VIII - Outras situações"                                    11,
   "I - Identificação do declarante - Actividade principal"     2,
   "I - Identificação do declarante - Nome completo"            1,
   "I - Identificação do declarante - Estado civil"             3,
   "I - Identificação do declarante - Regime de bens"           5,
   "I - Identificação do declarante - Nome completo do cônjuge" 4})

(def col-order-interesse-main-v2
  {"BID" 0,
   "V - Apoios ou benefícios" 9,
   "I - Identificação do declarante - Atividade principal" 2,
   "II - Cargo que exerce - Ano de" 7,
   "II - Cargo que exerce - Cargo" 6,
   "VIII - Outras situações" 11,
   "I - Identificação do declarante - Nome completo" 1,
   "I - Identificação do declarante - Estado civil" 3,
   "VI - Serviços prestados" 10,
   "I - Identificação do declarante - Nome completo do cônjuge" 4,
   "I - Identificação do declarante - Regime de bens" 5})


(def col-order-sociais-v1
  {"BID"                        1
   "Nome"                       2
   "Entidade"                   3
   "Cargo"                      4
   "Local da sede"              5
   "Natureza e área de serviço" 6})


(def col-order-sociais-v2
  {"BID"                        1
   "Nome"                       2
   "Entidade"                   3
   "Cargo"                      4
   "Local da sede"              5
   "Natureza e área de serviço" 6
   "Início"                     7
   "Fim"                        8})


(def col-order-sociedades-v1
  {"BID"                        1,
   "Nome"                       2,
   "Entidade"                   3,
   "Local da sede"              5,
   "Participação Social"        6,
   "Natureza e área de serviço" 4})


(def col-order-sociedades-v2
  {"BID"                 1,
   "Nome"                2,
   "Entidade"            3,
   "Local da sede"       5,
   "Participação social" 6,
   "Área da atividade"   4})


(def col-order-atividades-v2
  {"BID"        1
   "Nome"       2
   "Atividade"  3
   "Remunerada" 4
   "Ínicio"     7
   "Fim"        8})


(defn build-entry-interesse
  [db-content leg bid]
  (let [entry (get-in db-content [:int leg bid])]
    (->> entry
         (mapcat (fn [[k v]]
                   (cond
                     (vector? v)
                     []

                     (string? v)
                     [[k v]]

                     (map? v)
                     (mapv (fn [[kk vv]]
                             [(str k " - " kk) vv]) v))))
         (into {"BID" (str bid)}))))


(defn build-interesse-main-table
  [leg col-order]
  (let [db-content    @db
        col-order-vec (->col-order-vec col-order)]
    (->> (com-interesse leg)
         (mapv (partial build-entry-interesse db-content leg))
         (map (row-builder col-order-vec))
         (into [col-order-vec]))))


(defn build-entries-interesse
  [db-content leg col bid]
  (let [entry    (get-in db-content [:int leg bid])
        pre-rows (get entry col)
        nome     (get-in db-content [:bio bid "Nome"])]
    (when pre-rows
      (->> pre-rows
           (mapv (partial merge {"BID" (str bid) "Nome" nome}))))))


(defn build-interesse-table
  [leg col col-order]
  (let [db-content    @db
        col-order-vec (->col-order-vec col-order)]
    (->> (com-interesse leg)
         (mapcat (partial build-entries-interesse db-content leg col))
         (map (row-builder col-order-vec))
         (into [col-order-vec]))))


(defn export-interesses-v1!
  ([]
   (export-interesses-v1! "target/Interesse XI.xlsx"))
  ([filename]
   (x/save-workbook-into-file! filename
                               (x/create-workbook "Principal"
                                                  (build-interesse-main-table "XI" col-order-interesse-main-v1)

                                                  "Cargos Sociais"
                                                  (build-interesse-table "XI" "IV - Cargos Sociais" col-order-sociais-v1)

                                                  "Sociedades"
                                                  (build-interesse-table "XI" "VII - Sociedades" col-order-sociedades-v1)))))


(defn export-interesses-v2!
  ([]
   (export-interesses-v2! "target/Interesse %s.xlsx"))
  ([filename]
   (for [leg ["XII" "XIII"]]
     (x/save-workbook-into-file! (format filename leg)
                                 (x/create-workbook "Principal"
                                                    (build-interesse-main-table leg col-order-interesse-main-v2)

                                                    "Atividades"
                                                    (build-interesse-table leg "III - Atividades" col-order-atividades-v2)

                                                    "Cargos Sociais"
                                                    (build-interesse-table leg "IV - Cargos sociais" col-order-sociais-v2)

                                                    "Sociedades"
                                                    (build-interesse-table leg "VII - Sociedades" col-order-sociedades-v2))))))


; </editor-fold>

; </editor-fold>

; <editor-fold desc="REPL">

(comment

  (init-drivers! 16)
  (pmap click-cookies! (:pool @drivers))

  (def scraper (scrape-active! "2000-01-02" "2000-01-02"))
  (def scraper (scrape-efetivo-definitivo! "1999-10-25" "2019-10-24"))
  (def scraper (scrape-efetivo-temporario! "1999-10-25" "2019-10-24"))

  (def scraper (scrape-bios!))

  (cancel-scrape! scraper)

  (init-drivers! 1)
  (pmap click-cookies! (:pool @drivers))
  (def driver (first (:pool @drivers)))

  )

; </editor-fold>