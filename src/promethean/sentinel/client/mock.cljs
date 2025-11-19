
(defn create-mock-client
  "Create a mock Sentinel client for development/testing"
  [subscription-fn]
  (let [mock-events (atom [])
        mock-interval (atom nil)]

    {:subscribe (fn [fn]
                  (reset! mock-events [])
                  ;; Simulate events every 5 seconds for testing
                  (reset! mock-interval
                          (js/setInterval
                           (fn []
                             (let [mock-event {:type :synthetic
                                               :path "/mock/path"
                                               :relative "mock/path"
                                               :watcher {:key :mock :path "/mock" :abs-path "/mock"}
                                               :rule {:id :mock-test :on :change}
                                               :event {:type :change :path "/mock/path"}
                                               :ts (.now js/Date)}]
                               (subscription-fn mock-event)))
                           5000)))
     :close (fn []
              (when @mock-interval
                (js/clearInterval @mock-interval))
              (reset! mock-events []))}))

(defn create-mock-client-with-events
  "Create a mock client with predefined events for testing"
  [subscription-fn mock-events-list]
  (let [event-index (atom 0)
        mock-interval (atom nil)]

    {:subscribe (fn [fn]
                  (reset! event-index 0)
                  ;; Simulate events from provided list
                  (reset! mock-interval
                          (js/setInterval
                           (fn []
                             (when (< @event-index (count mock-events-list))
                               (let [mock-event (nth mock-events-list @event-index)]
                                 (reset! event-index (inc @event-index))
                                 (subscription-fn mock-event))))
                           1000)))
     :close (fn []
              (when @mock-interval
                (js/clearInterval @mock-interval))
              (reset! event-index 0))}))
