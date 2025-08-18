package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type HTTPBrokerClient struct {
	brokers map[string]string
	client  *http.Client
}

func NewHTTPBrokerClient() *HTTPBrokerClient {
	return &HTTPBrokerClient{
		brokers: make(map[string]string),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *HTTPBrokerClient) AddBroker(brokerID, address string) {
	c.brokers[brokerID] = address
}

func (c *HTTPBrokerClient) FetchRecords(brokerID, topic string, partition int, offset int64, maxBytes int) (*FetchResponse, error) {
	address, exists := c.brokers[brokerID]
	if !exists {
		return nil, fmt.Errorf("Cannot find broker %s", brokerID)
	}

	url := fmt.Sprintf("%s/fetch_replica?topic=%s&partition=%d&offset=%d&maxBytes=%d", address, topic, partition, offset, maxBytes)

	res, err := c.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var fetchRes FetchResponse

	if err := json.NewDecoder(res.Body).Decode(&fetchRes); err != nil {
		return nil, err
	}

	return &fetchRes, nil
}

func (c *HTTPBrokerClient) UpdateFollowerState(brokerID, topic string, partition int, followerID string, fetchOffset, longEndOffset int64) error {
	address, exists := c.brokers[brokerID]
	if !exists {
		return fmt.Errorf("Cannot find broker %s", brokerID)
	}

	url := fmt.Sprintf("%s/update-follower?topic=%s&partition=%d&fetchoffset=%d&longendoffset=%d", address, topic, partition, fetchOffset, longEndOffset)
	res, err := c.client.Get(url)
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to update state")
	}

	return nil
}
