package esdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

func (client *Client) httpListAllPersistentSubscriptions(options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfo, error) {
	body, err := client.httpExecute("GET", "/subscriptions", options.Authenticated, nil)

	if err != nil {
		return nil, err
	}

	var subs []PersistentSubscriptionInfoHttpJson

	err = json.Unmarshal(body, &subs)

	if err != nil {
		return nil, &Error{code: ErrorParsing, err: fmt.Errorf("error when parsing JSON payload: %w", err)}
	}

	var infos []PersistentSubscriptionInfo

	for _, src := range subs {
		info, err := fromHttpJsonInfo(src)
		if err != nil {
			return nil, err
		}

		infos = append(infos, info)
	}

	return infos, nil
}

func (client *Client) httpListPersistentSubscriptionsForStream(streamName string, options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfo, error) {
	body, err := client.httpExecute("GET", fmt.Sprintf("/subscriptions/%s", streamName), options.Authenticated, nil)

	if err != nil {
		return nil, err
	}

	var subs []PersistentSubscriptionInfoHttpJson

	err = json.Unmarshal(body, &subs)

	if err != nil {
		return nil, &Error{code: ErrorParsing, err: fmt.Errorf("error when parsing JSON payload: %w", err)}
	}

	var infos []PersistentSubscriptionInfo

	for _, src := range subs {
		info, err := fromHttpJsonInfo(src)
		if err != nil {
			return nil, err
		}

		infos = append(infos, info)
	}

	return infos, nil
}

func (client *Client) httpGetPersistentSubscriptionInfo(streamName string, groupName string, options GetPersistentSubscriptionOptions) (*PersistentSubscriptionInfo, error) {
	body, err := client.httpExecute("GET", fmt.Sprintf("/subscriptions/%s/%s/info", streamName, groupName), options.Authenticated, nil)

	if err != nil {
		return nil, err
	}

	var src PersistentSubscriptionInfoHttpJson

	err = json.Unmarshal(body, &src)

	if err != nil {
		return nil, &Error{code: ErrorParsing, err: fmt.Errorf("error when parsing JSON payload: %w", err)}
	}

	info, err := fromHttpJsonInfo(src)

	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (client *Client) httpReplayParkedMessages(streamName string, groupName string, options ReplayParkedMessagesOptions) error {
	params := &httpParams{
		headers: []keyvalue{newKV("content-length", "0")},
	}

	if options.StopAt != 0 {
		params.queries = append(params.queries, newKV("stopAt", strconv.Itoa(options.StopAt)))
	}

	url := fmt.Sprintf("/subscriptions/%s/%s/replayParked", streamName, groupName)
	_, err := client.httpExecute("POST", url, options.Authenticated, params)

	return err
}

func (client *Client) getBaseUrl() (string, error) {
	handle, err := client.grpcClient.getConnectionHandle()

	if err != nil {
		return "", err
	}

	var protocol string
	if client.Config.DisableTLS {
		protocol = "http"
	} else {
		protocol = "https"
	}

	return fmt.Sprintf("%s://%s", protocol, handle.connection.Target()), nil
}

type keyvalue struct {
	key   string
	value string
}

func newKV(key string, value string) keyvalue {
	return keyvalue{
		key:   key,
		value: value,
	}
}

type httpParams struct {
	queries []keyvalue
	headers []keyvalue
}

func (client *Client) httpExecute(method string, path string, auth *Credentials, params *httpParams) ([]byte, error) {
	baseUrl, err := client.getBaseUrl()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}

	req, err := http.NewRequest(method, fmt.Sprintf("%s%s", baseUrl, path), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("content-type", "application/json")

	if params != nil {
		if params.headers != nil {
			for i := range params.headers {
				tuple := params.headers[i]
				req.Header.Add(tuple.key, tuple.value)
			}
		}

		if params.queries != nil {
			query := req.URL.Query()
			for i := range params.queries {
				tuple := params.queries[i]
				query.Add(tuple.key, tuple.value)
			}
			req.URL.RawQuery = query.Encode()
		}
	}

	var creds *Credentials
	if auth != nil {
		creds = auth
	} else {
		if client.Config.Username != "" {
			creds = &Credentials{
				Login:    client.Config.Username,
				Password: client.Config.Password,
			}
		}
	}

	if creds != nil {
		req.SetBasicAuth(creds.Login, creds.Password)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 600 {
		switch resp.StatusCode {
		case 401:
			return nil, &Error{code: ErrorAccessDenied}
		case 404:
			return nil, &Error{code: ErrorResourceNotFound}
		default:
			{
				if resp.StatusCode >= 500 && resp.StatusCode < 600 {
					return nil, &Error{code: ErrorInternalServer, err: fmt.Errorf("server returned a '%v' response", resp.StatusCode)}
				}

				return nil, &Error{code: ErrorInternalClient, err: fmt.Errorf("unexpected response code '%v'", resp.StatusCode)}
			}
		}
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &Error{code: ErrorUnknown, err: err}
	}

	return body, nil
}

func fromHttpJsonInfo(src PersistentSubscriptionInfoHttpJson) (PersistentSubscriptionInfo, error) {
	var settings *SubscriptionSettings
	var stats *PersistentSubscriptionStats
	info := PersistentSubscriptionInfo{}

	if src.Config != nil {
		settings = &SubscriptionSettings{}
		settings.ResolveLinkTos = src.Config.ResolveLinkTos
		settings.ExtraStatistics = src.Config.ExtraStatistics
		settings.MessageTimeout = int32(src.Config.MessageTimeout)
		settings.MaxRetryCount = int32(src.Config.MaxRetryCount)
		settings.LiveBufferSize = int32(src.Config.LiveBufferSize)
		settings.ReadBatchSize = int32(src.Config.ReadBatchSize)
		settings.CheckpointAfter = int32(src.Config.CheckpointAfter)
		settings.CheckpointLowerBound = int32(src.Config.CheckpointLowerBound)
		settings.CheckpointUpperBound = int32(src.Config.CheckpointUpperBound)
		settings.MaxSubscriberCount = int32(src.Config.MaxSubscriberCount)
		settings.ConsumerStrategyName = ConsumerStrategy(src.Config.ConsumerStrategyName)

		if src.EventStreamId == "$all" {
			settings.StartFrom = src.Config.StartPosition
		} else {
			settings.StartFrom = src.Config.StartFrom
		}

		info.Settings = settings
	}

	if src.Config.ExtraStatistics {
		stats = &PersistentSubscriptionStats{}
		stats.AveragePerSecond = int64(src.AverageItemsPerSecond)
		stats.TotalItems = src.TotalItemsProcessed
		stats.CountSinceLastMeasurement = src.CountSinceLastMeasurement
		stats.ReadBufferCount = src.ReadBufferCount
		stats.LiveBufferCount = src.LiveBufferCount
		stats.RetryBufferCount = src.RetryBufferCount
		stats.TotalInFlightMessages = src.TotalInFlightMessages
		stats.OutstandingMessagesCount = src.OutstandingMessagesCount
		stats.ParkedMessagesCount = src.ParkedMessageCount

		if src.EventStreamId == "$all" {
			if src.LastCheckpointedEventPosition != "" {
				stats.LastKnownEventRevision = src.LastKnownEventPosition
			}
		} else {
			stats.LastCheckpointedEventRevision = src.LastKnownEventNumber
		}

		info.Stats = stats
	}

	info.EventSource = src.EventStreamId
	info.GroupName = src.GroupName
	info.Status = src.Status
	info.Connections = src.Connections

	return info, nil
}
