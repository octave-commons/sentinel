(ns promethean.sentinel.client.node
  "Node.js Sentinel client for subscribing to Sentinel events"
  (:require [promethean.sentinel.events :as events]
            ))

;; External dependencies
(def messaging (js/require "@promethean-os/messaging"))
(def logger (js/require "@promethean-os/logger"))

(def default-topics ["sentinel.synthetic.*"])

(def initial-state
  {:connected false
   :messaging-ctx nil
   :subscriptions []
   :reconnect-timer nil
   :backoff-ms 1000
   :max-backoff-ms 30000
   :logger nil
   :client-config nil})

(defonce client-state (atom initial-state))

(defn log
  "Log helper function"
  [level msg ctx]
  (let [{:keys [logger]} @client-state
        ctx-js (clj->js (or ctx {}))
        level-name (name level)]
    (cond
      (and logger (.-log logger)) (.log logger level-name msg ctx-js)
      (and logger (.-info logger)) (.info logger msg ctx-js)
      :else (.log js/console level-name msg ctx-js)))
  nil)

(defn validate-event-payload
  "Validate incoming event payload and log if malformed"
  [payload]
  (try
    (let [event (js->clj payload :keywordize-keys true)]
      (if (events/validate-event event)
        event
        (do
          (log :warn "malformed event payload dropped" {:payload payload})
          nil)))
    (catch :default e
      (log :error "failed to parse event payload" {:payload payload :error e})
      nil)))

(defn create-backoff-delay
  "Calculate exponential backoff delay"
  [current-backoff max-backoff]
  (min (* current-backoff 2) max-backoff))

(defn clear-reconnect!
  []
  (when-let [timer (:reconnect-timer @client-state)]
    (js/clearTimeout timer)
    (swap! client-state assoc :reconnect-timer nil)))

(defn reset-state!
  [cfg]
  (reset! client-state (merge initial-state
                              {:backoff-ms (:backoff-ms cfg)
                               :max-backoff-ms (:max-backoff-ms cfg)
                               :logger (:logger cfg)
                               :client-config cfg})))

(defn schedule-reconnect
  "Schedule reconnection with exponential backoff"
  [client-config reconnect-fn]
  (let [{:keys [backoff-ms max-backoff-ms]} @client-state
        new-backoff (create-backoff-delay backoff-ms max-backoff-ms)]
    (swap! client-state assoc :backoff-ms new-backoff)
    (let [timer-id (js/setTimeout
                    (fn []
                      (log :info "attempting reconnect" {:backoff-ms new-backoff})
                      (reconnect-fn))
                    new-backoff)]
      (swap! client-state assoc :reconnect-timer timer-id))))

(defn connect-messaging
  "Connect to messaging transport"
  [client-config]
  (try
    (let [{:keys [url reconnect logger]} client-config
          createRabbitContext (when messaging (.-createRabbitContext messaging))
          ctx (when createRabbitContext (createRabbitContext))]

      (if ctx
        (do
          (swap! client-state assoc
                 :connected true
                 :messaging-ctx ctx
                 :logger logger
                 :backoff-ms 1000) ; Reset backoff on successful connect
          (log :info "sentinel client connected" {:url url})
          {:connected true})
        (do
          (log :error "failed to create messaging context" {:url url})
          (when reconnect
            (schedule-reconnect client-config))
          {:connected false})))
    (catch :default e
      (log :error "messaging connection failed" {:error e})
      (when (:reconnect client-config)
        (schedule-reconnect client-config))
      {:connected false})))



(defn handle-message
  "Handle incoming message from messaging transport"
  [topic payload subscription-fn]
  (when-let [event (validate-event-payload payload)]
    (log :debug "received sentinel event" {:topic topic :event event})
    (subscription-fn event)))

(defn subscribe-to-topic
  "Subscribe to a specific topic"
  [ctx topic subscription-fn]
  (try
    (let [closer (.subscribe ctx topic
                             (fn [payload]
                               (handle-message topic payload subscription-fn)))]
      (log :info "subscribed to sentinel topic" {:topic topic})
      closer)
    (catch :default e
      (log :error "failed to subscribe to topic" {:topic topic :error e})
      nil)))

(defn subscribe
  "Subscribe to Sentinel events"
  [client-config subscription-fn]
  (let [{:keys [topics]} client-config
        {:keys [messaging-ctx connected]} @client-state]

    (if connected
        (let [closers (->> topics
                           (map #(subscribe-to-topic messaging-ctx % subscription-fn))
                           (remove nil))]
          (swap! client-state assoc :subscriptions closers)
          {:subscribe (fn [new-fn]
                        ;; Update subscription function for existing subscriptions
                        (doseq [topic topics]
                          (subscribe-to-topic messaging-ctx topic new-fn)))
           :close (fn []
                    (log :info "closing sentinel client" {})
                    ;; Cancel reconnect timer
                    (when-let [timer (:reconnect-timer @client-state)]
                      (js/clearInterval timer))
                    ;; Close all subscriptions
                    (doseq [closer (:subscriptions @client-state)]
                      (when closer (.close closer)))
                    ;; Close messaging context
                    (when messaging-ctx
                      (.close messaging-ctx))
                    (swap! client-state assoc
                           :connected false
                           :messaging-ctx nil
                           :subscriptions []
                           :reconnect-timer nil))})
        (do
          (log :error "cannot subscribe - client not connected" {})
          nil))))

(defn createSentinelClient
  "Create a Sentinel client instance"
  [client-config]
  (let [default-config {:url (or (aget (.-env js/process) "SENTINEL_URL") "amqp://localhost")
                         :topics ["sentinel.synthetic.*"]
                         :reconnect true
                         :logger nil
                        }
        config (merge default-config client-config)]

    ;; Initialize client state
    (reset! client-state {:connected false
                          :messaging-ctx nil
                          :subscriptions []
                          :reconnect-timer nil
                          :backoff-ms 1000
                          :max-backoff-ms 30000
                          :logger (:logger config)})

    ;; Connect to messaging
    (let [connection-result (connect-messaging config)]
      (if (:connected connection-result)
        (subscribe config (:subscription-fn client-config))
        (do
          (log :error "failed to create sentinel client" {:config config})
          nil)))))

(defn createSentinelClientWithSubscription
  "Create a Sentinel client instance with subscription function"
  [client-config subscription-fn]
  (let [config-with-fn (assoc client-config :subscription-fn subscription-fn)]
    (createSentinelClient config-with-fn)))
