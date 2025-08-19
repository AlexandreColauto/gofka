package broker

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/alexandrecolauto/gofka/model"
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

func (c *HTTPBrokerClient) UpdateBrokers(brokers []model.BrokerInfo) {
	brs := make(map[string]string)
	c.brokers = brs
	for _, broker := range brokers {
		if broker.Alive {
			c.brokers[broker.ID] = broker.Address
		}
	}
	fmt.Println("New brokers: ", c.brokers)
}

func (c *HTTPBrokerClient) FetchRecords(brokerID, topic string, partition int, offset int64, maxBytes int) (*FetchResponse, error) {
	address, exists := c.brokers[brokerID]
	if !exists {
		return nil, fmt.Errorf("Cannot find broker %s", brokerID)
	}

	url := fmt.Sprintf("http://%s/fetch-replica?topic=%s&partition=%d&offset=%d&maxBytes=%d", address, topic, partition, offset, maxBytes)

	res, err := c.client.Get(url)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("Invalid status: %d, %s", res.StatusCode, b)
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

	url := fmt.Sprintf("http://%s/update-follower?topic=%s&partition=%d&fetchoffset=%d&longendoffset=%d&followerid=%s", address, topic, partition, fetchOffset, longEndOffset, followerID)

	res, err := c.client.Get(url)
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to update state")
	}

	return nil
}
