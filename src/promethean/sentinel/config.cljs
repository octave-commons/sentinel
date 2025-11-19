(ns promethean.sentinel.config
  "Configuration handling for Sentinel client"
  (:require [clojure.edn :as edn]
            [clojure.string :as str]))

(def fs (js/require "fs"))
(def path (js/require "path"))

(def default-config
  {:url nil
   :topics ["sentinel.synthetic.*"]
   :reconnect true
   :reconnect-timeout 30000
   :backoff-ms 1000
   :max-backoff-ms 30000
   :mock false
   :logger nil
   :timeout 30000
   :validate-events true
   :log-level :info
   :config-path nil
   :watcher-root nil
   :include-raw false
   :debounce-ms 1000
   :max-queue-size 1000
   :heartbeat-interval 30000
   :connection-name "sentinel-client"
   :prefetch-count 10
   :retry-attempts 3
   :retry-delay 1000})

(defn env-var
  "Get environment variable with optional default"
  [var-name default-val]
  (or (aget (.-env js/process) var-name) default-val))

(defn parse-env-var
  "Parse environment variable to appropriate type"
  [var-name default-val parser]
  (try
    (let [val (env-var var-name default-val)]
      (if (and val (not= val default-val))
        (parser val)
        default-val))
    (catch :default _
      default-val)))

(defn parse-boolean-val
  "Parse string to boolean"
  [val]
  (and val (not= "false" (str/lower-case val))))

(defn load-config-file
  "Load configuration from EDN file"
  [config-path]
  (when (and config-path (.existsSync fs config-path))
    (try
      (-> (.readFileSync fs config-path "utf8")
          (edn/read-string))
      (catch :default e
        (js/console.warn "Failed to load config file:" config-path e)
        nil))))

(defn merge-config
  "Merge configuration with precedence: CLI args > env vars > config file > defaults"
  [cli-args env-vars config-file defaults]
  (merge defaults
          config-file
          env-vars
          cli-args))

(defn load-env-config
  "Load configuration from environment variables"
  []
  {:url (env-var "SENTINEL_URL" nil)
   :topics (when-let [topics (env-var "SENTINEL_TOPICS" nil)]
             (str/split topics #","))
   :reconnect (parse-env-var "SENTINEL_RECONNECT" true parse-boolean-val)
   :reconnect-timeout (parse-env-var "SENTINEL_RECONNECT_TIMEOUT" 30000 js/parseInt)
   :backoff-ms (parse-env-var "SENTINEL_BACKOFF_MS" 1000 js/parseInt)
   :max-backoff-ms (parse-env-var "SENTINEL_MAX_BACKOFF_MS" 30000 js/parseInt)
   :mock (parse-env-var "SENTINEL_MOCK" false parse-boolean-val)
   :timeout (parse-env-var "SENTINEL_TIMEOUT" 30000 js/parseInt)
   :log-level (keyword (env-var "SENTINEL_LOG_LEVEL" "info"))
   :config-path (env-var "SENTINEL_CONFIG_PATH" nil)
   :watcher-root (env-var "SENTINEL_WATCHER_ROOT" nil)
   :include-raw (parse-env-var "SENTINEL_INCLUDE_RAW" false parse-boolean-val)
   :debounce-ms (parse-env-var "SENTINEL_DEBOUNCE_MS" 1000 js/parseInt)
   :max-queue-size (parse-env-var "SENTINEL_MAX_QUEUE_SIZE" 1000 js/parseInt)
   :heartbeat-interval (parse-env-var "SENTINEL_HEARTBEAT_INTERVAL" 30000 js/parseInt)
   :connection-name (env-var "SENTINEL_CONNECTION_NAME" "sentinel-client")
   :prefetch-count (parse-env-var "SENTINEL_PREFETCH_COUNT" 10 js/parseInt)
   :retry-attempts (parse-env-var "SENTINEL_RETRY_ATTEMPTS" 3 js/parseInt)
   :retry-delay (parse-env-var "SENTINEL_RETRY_DELAY" 1000 js/parseInt)})

(defn parse-cli-args
  "Parse command line arguments for Sentinel client"
  [args]
  (loop [remaining args
         result {}]
    (if (empty? remaining)
      result
      (let [arg (first remaining)
            rest (rest remaining)]
        (cond
          (and (= arg "--sentinel-url") (seq rest))
          (recur (rest rest) (assoc result :url (first rest)))
          
          (and (= arg "--sentinel-topics") (seq rest))
          (recur (rest rest) (assoc result :topics (str/split (first rest) #",")))
          
          (= arg "--sentinel")
          (recur rest (assoc result :sentinel-enabled true))
          
          (= arg "--mock")
          (recur rest (assoc result :mock true))
          
          (= arg "--no-reconnect")
          (recur rest (assoc result :reconnect false))
          
          (= arg "--include-raw")
          (recur rest (assoc result :include-raw true))
          
          (and (= arg "--config") (seq rest))
          (recur (rest rest) (assoc result :config-path (first rest)))
          
          (and (= arg "--watcher-root") (seq rest))
          (recur (rest rest) (assoc result :watcher-root (first rest)))
          
          (and (= arg "--log-level") (seq rest))
          (recur (rest rest) (assoc result :log-level (keyword (first rest))))
          
          (and (= arg "--timeout") (seq rest))
          (recur (rest rest) (assoc result :timeout (js/parseInt (first rest))))
          
          :else
          (recur rest result))))))

(defn resolve-config-path
  "Resolve configuration file path"
  [config-path]
  (cond
    (nil? config-path) nil
    (.isAbsolute path config-path) config-path
    :else (.resolve path config-path)))

(defn validate-config
  "Validate configuration and return errors if any"
  [config]
  (let [errors []]
    (cond-> errors
      (and (:url config) (not (string? (:url config))))
      (conj "URL must be a string")
      
      (and (:topics config) (not (sequential? (:topics config))))
      (conj "Topics must be an array")
      
      (and (:timeout config) (not (number? (:timeout config))))
      (conj "Timeout must be a number")
      
      (and (:backoff-ms config) (not (number? (:backoff-ms config))))
      (conj "Backoff MS must be a number")
      
      (and (:max-backoff-ms config) (not (number? (:max-backoff-ms config))))
      (conj "Max backoff MS must be a number")
      
      (and (:debounce-ms config) (not (number? (:debounce-ms config))))
      (conj "Debounce MS must be a number")
      
      (and (:max-queue-size config) (not (number? (:max-queue-size config))))
      (conj "Max queue size must be a number")
      
      (and (:heartbeat-interval config) (not (number? (:heartbeat-interval config))))
      (conj "Heartbeat interval must be a number")
      
      (and (:prefetch-count config) (not (number? (:prefetch-count config))))
      (conj "Prefetch count must be a number")
      
      (and (:retry-attempts config) (not (number? (:retry-attempts config))))
      (conj "Retry attempts must be a number")
      
      (and (:retry-delay config) (not (number? (:retry-delay config))))
      (conj "Retry delay must be a number"))))

(defn load-config
  "Load complete configuration from all sources"
  [cli-args]
  (let [cli-config (parse-cli-args cli-args)
        env-config (load-env-config)
        config-path (some-> (:config-path cli-config) 
                            resolve-config-path)
        file-config (when config-path (load-config-file config-path))
        merged (merge-config cli-config env-config file-config default-config)
        errors (validate-config merged)]
    
    (when (seq errors)
      (throw (ex-info "Configuration validation failed" 
                      {:errors errors :config merged})))
    
    merged))

(defn get-topics
  "Get topics to subscribe to based on configuration"
  [config]
  (let [base-topics (:topics config)]
    (if (:include-raw config)
      (conj base-topics "sentinel.fs.raw")
      base-topics)))

(defn create-client-config
  "Create client configuration from general config"
  [config]
  {:url (:url config)
   :topics (get-topics config)
   :reconnect (:reconnect config)
   :logger (:logger config)
   :mock (:mock config)
   :timeout (:timeout config)
   :backoff-ms (:backoff-ms config)
   :max-backoff-ms (:max-backoff-ms config)
   :validate-events (:validate-events config)
   :debounce-ms (:debounce-ms config)
   :max-queue-size (:max-queue-size config)
   :heartbeat-interval (:heartbeat-interval config)
   :connection-name (:connection-name config)
   :prefetch-count (:prefetch-count config)
   :retry-attempts (:retry-attempts config)
   :retry-delay (:retry-delay config)})