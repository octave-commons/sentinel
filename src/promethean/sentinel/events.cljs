(ns promethean.sentinel.events
  "Event schema definitions and validation for Sentinel events")

;; Event type definitions
(def event-types #{:fs :synthetic})

;; Base event schema
(def base-event-schema
  {:type keyword?
   :path string?
   :relative (some-fn string? nil?)
   :watcher map?
   :ts number?})

;; Watcher schema
(def watcher-schema
  {:key any?
   :path string?
   :abs-path string?})

;; Rule schema (for synthetic events)
(def rule-schema
  {:id (some-fn keyword? string?)
   :on (some-fn keyword? string? nil?)
   :glob (some-fn string? nil?)
   :debounce-ms (some-fn number? nil?)
   :when (some-fn map? nil?)})

;; Complete event schemas
(def fs-event-schema
  (merge base-event-schema
         {:type (fn [t] (= t :fs))
          :watcher watcher-schema}))

(def synthetic-event-schema
  (merge base-event-schema
         {:type (fn [t] (= t :synthetic))
          :watcher watcher-schema
          :rule rule-schema
          :event map?}))

(defn validate-event
  "Validate an event against the appropriate schema"
  [event]
  (let [event-type (:type event)]
    (cond
      (= event-type :fs)
      (and (= (:type event) :fs)
           (string? (:path event))
           (or (string? (:relative event)) (nil? (:relative event)))
           (map? (:watcher event))
           (number? (:ts event)))
      
      (= event-type :synthetic)
      (and (= (:type event) :synthetic)
           (string? (:path event))
           (or (string? (:relative event)) (nil? (:relative event)))
           (map? (:watcher event))
           (map? (:rule event))
           (map? (:event event))
           (number? (:ts event)))
      
      :else
      false)))

(defn create-fs-event
  "Create a filesystem event"
  [{:keys [path relative watcher]
    :or {relative nil}}]
  {:type :fs
   :path path
   :relative relative
   :watcher watcher
   :ts (.now js/Date)})

(defn create-synthetic-event
  "Create a synthetic event"
  [{:keys [path relative watcher rule event]
    :or {relative nil}}]
  {:type :synthetic
   :path path
   :relative relative
   :watcher watcher
   :rule rule
   :event event
   :ts (.now js/Date)})

(defn event->topic
  "Convert an event to its messaging topic"
  [event]
  (case (:type event)
    :fs "sentinel.fs.raw"
    :synthetic (str "sentinel.synthetic." 
                   (when-let [rule-id (get-in event [:rule :id])]
                     (name rule-id))
                   ".")
    nil))

(defn topic-pattern
  "Get topic pattern for subscription"
  [event-type]
  (case event-type
    :fs "sentinel.fs.raw"
    :synthetic "sentinel.synthetic.*"
    nil))