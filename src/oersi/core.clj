(ns oersi.core
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [nostr.edufeed :as edufeed]))

(defn fetch-data [pit-id last-sort-value]
  (let [url "https://oersi.org/resources/api-internal/search/_search?pretty"
        _ (println last-sort-value)
        query (merge {:size 1000
                      :query {:match {:mainEntityOfPage.provider.name "twillo"}}
                      :pit {:id pit-id
                            :keep_alive "1m"}
                      :sort [{:id "asc"}]
                      :track_total_hits true}
                     (when last-sort-value 
                       {:search_after last-sort-value}))
        response (http/post url
                            {:accept :json
                             :content-type :json
                             :body (json/generate-string query)})]
    (json/parse-string (:body response) true)))

(defn save-hits [hits]
  (println "Got " (count hits) "results"))

;; Function to save a batch of hits to a JSON line file
(defn save-to-jsonl [data file-path]
  (with-open [writer (io/writer file-path :append true)]
    (doseq [record (:hits (:hits data))]
      (.write writer (str (json/generate-string record) "\n")))))

(defn crawl-oersi [args]
  (println "Crawl oersi" args)
  (let [output-file "oersi_data.jsonl"
        pit (http/post "https://oersi.org/resources/api-internal/search/oer_data/_pit?keep_alive=1m&pretty"
                       {:accept :json})
        pit-id (-> pit :body (#(json/parse-string % true)) :id)]
    (println "Generated PIT: " pit-id)
    (loop [last-sort-value nil]
      (let [body (fetch-data pit-id last-sort-value)
            hits (-> body :hits :hits)]
        (save-to-jsonl body output-file)
        (if-not (empty? hits)
          (recur (get (last hits) :sort))
          (println "no more records to fetch"))))))

(defn search-oersi [args]
  (let [url "https://oersi.org/resources/api-internal/search/oer_data/_search?pretty"
        query-2 {:size 1
                 :from 0
                 :query {:match_all {}}}
        query {:size 20
               :from 0
               :query {:multi_match {:query "Klimawandel"
                                     :fields ["name", "description", "keywords"]}}
               :sort [{:id "asc"}]}
        response (http/post url
                            {:content-type :json
                             :accept :json
                             :body (json/generate-string query-2)})]
    (println response)))

;; FIXME read file, and then process line by line
(defn export-to-nostr [args]
  (println args)
  (let [file-path (:file-path args)
        _ (println "file path" file-path)]
    (edufeed/transform-amb-to-30142-event)))

(defn -main []
  (println "Hello world"))
