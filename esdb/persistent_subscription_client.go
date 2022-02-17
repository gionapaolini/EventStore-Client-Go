package esdb

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type persistentClient struct {
	inner                        *grpcClient
	persistentSubscriptionClient persistent.PersistentSubscriptionsClient
}

func (client *persistentClient) ConnectToPersistentSubscription(
	ctx context.Context,
	handle connectionHandle,
	bufferSize int32,
	streamName string,
	groupName string,
	auth *Credentials,
) (*PersistentSubscription, error) {
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}

	deadline := time.Now().Add(time.Duration(math.MaxInt64))
	ctx, cancel := context.WithDeadline(ctx, deadline)
	readClient, err := client.persistentSubscriptionClient.Read(ctx, callOptions...)
	if err != nil {
		defer cancel()
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}

	err = readClient.Send(toPersistentReadRequest(bufferSize, groupName, []byte(streamName)))
	if err != nil {
		defer cancel()
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}
	switch readResult.Content.(type) {
	case *persistent.ReadResp_SubscriptionConfirmation_:
		{
			asyncConnection := NewPersistentSubscription(
				readClient,
				readResult.GetSubscriptionConfirmation().SubscriptionId,
				cancel)

			return asyncConnection, nil
		}
	}

	defer cancel()
	return nil, &Error{code: ErrorUnknown, err: fmt.Errorf("persistent subscription confirmation error")}
}

func (client *persistentClient) CreateStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
	position StreamPosition,
	settings SubscriptionSettings,
	auth *Credentials,
) error {
	createSubscriptionConfig := createPersistentRequestProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Create(ctx, createSubscriptionConfig, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) CreateAllSubscription(
	ctx context.Context,
	handle connectionHandle,
	groupName string,
	position AllPosition,
	settings SubscriptionSettings,
	filter *SubscriptionFilterOptions,
	auth *Credentials,
) error {
	protoConfig, err := createPersistentRequestAllOptionsProto(groupName, position, settings, filter)
	if err != nil {
		return err
	}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err = client.persistentSubscriptionClient.Create(ctx, protoConfig, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) UpdateStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
	position StreamPosition,
	settings SubscriptionSettings,
	auth *Credentials,
) error {
	updateSubscriptionConfig := updatePersistentRequestStreamProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) UpdateAllSubscription(
	ctx context.Context,
	handle connectionHandle,
	groupName string,
	position AllPosition,
	settings SubscriptionSettings,
	auth *Credentials,
) error {
	updateSubscriptionConfig := updatePersistentRequestAllOptionsProto(groupName, position, settings)

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) DeleteStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
	auth *Credentials,
) error {
	deleteSubscriptionOptions := deletePersistentRequestStreamProto(streamName, groupName)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) DeleteAllSubscription(ctx context.Context, handle connectionHandle, groupName string, auth *Credentials) error {
	deleteSubscriptionOptions := deletePersistentRequestAllOptionsProto(groupName)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) listPersistentSubscriptions(ctx context.Context, handle connectionHandle, streamName *string, options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfoHttpJson, error) {
	listOptions := &persistent.ListReq_Options{}

	if streamName == nil {
		listOptions.ListOption = &persistent.ListReq_Options_ListAllSubscriptions{
			ListAllSubscriptions: &shared.Empty{},
		}
	} else if *streamName == "$all" {
		listOptions.ListOption = &persistent.ListReq_Options_ListForStream{
			ListForStream: &persistent.ListReq_StreamOption{
				StreamOption: &persistent.ListReq_StreamOption_All{
					All: &shared.Empty{},
				},
			},
		}
	} else {
		listOptions.ListOption = &persistent.ListReq_Options_ListForStream{
			ListForStream: &persistent.ListReq_StreamOption{
				StreamOption: &persistent.ListReq_StreamOption_Stream{
					Stream: &shared.StreamIdentifier{StreamName: []byte(*streamName)},
				},
			},
		}
	}

	listReq := &persistent.ListReq{
		Options: listOptions,
	}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if options.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: options.Authenticated.Login,
			password: options.Authenticated.Password,
		}))
	}

	resp, err := client.persistentSubscriptionClient.List(ctx, listReq, callOptions...)

	if err != nil {
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}

	var infos []PersistentSubscriptionInfoHttpJson
	for _, wire := range resp.GetSubscriptions() {
		info, err := subscriptionInfoFromWire(wire)

		if err != nil {
			return nil, err
		}

		infos = append(infos, *info)
	}

	return infos, nil
}

func (client *persistentClient) getPersistentSubscriptionInfo(ctx context.Context, handle connectionHandle, streamName *string, groupName string, options GetPersistentSubscriptionOptions) (*PersistentSubscriptionInfoHttpJson, error) {
	getInfoOptions := &persistent.GetInfoReq_Options{}

	if streamName == nil {
		getInfoOptions.StreamOption = &persistent.GetInfoReq_Options_All{All: &shared.Empty{}}
	} else {
		getInfoOptions.StreamOption = &persistent.GetInfoReq_Options_StreamIdentifier{
			StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(*streamName)},
		}
	}

	getInfoOptions.GroupName = groupName

	getInfoReq := &persistent.GetInfoReq{Options: getInfoOptions}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if options.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: options.Authenticated.Login,
			password: options.Authenticated.Password,
		}))
	}

	resp, err := client.persistentSubscriptionClient.GetInfo(ctx, getInfoReq, callOptions...)
	if err != nil {
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}

	info, err := subscriptionInfoFromWire(resp.SubscriptionInfo)

	if err != nil {
		return nil, err
	}

	return info, nil
}

func (client *persistentClient) replayParkedMessages(ctx context.Context, handle connectionHandle, streamName *string, options ReplayParkedMessagesOptions) error {
	replayOptions := &persistent.ReplayParkedReq_Options{}

	if streamName == nil {
		replayOptions.StreamOption = &persistent.ReplayParkedReq_Options_All{All: &shared.Empty{}}
	} else {
		replayOptions.StreamOption = &persistent.ReplayParkedReq_Options_StreamIdentifier{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(*streamName),
			},
		}
	}

	if options.StopAt == 0 {
		replayOptions.StopAtOption = &persistent.ReplayParkedReq_Options_NoLimit{NoLimit: &shared.Empty{}}
	} else {
		replayOptions.StopAtOption = &persistent.ReplayParkedReq_Options_StopAt{StopAt: int64(options.StopAt)}
	}

	replayReq := &persistent.ReplayParkedReq{Options: replayOptions}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if options.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: options.Authenticated.Login,
			password: options.Authenticated.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.ReplayParked(ctx, replayReq, callOptions...)

	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func subscriptionInfoFromWire(wire *persistent.SubscriptionInfo) (*PersistentSubscriptionInfoHttpJson, error) {
	lastKnownEventNumber, err := strconv.Atoi(wire.LastKnownEventPosition)

	if err != nil {
		return nil, &Error{
			code: ErrorParsing,
			err:  fmt.Errorf("error when parsing LastKnownEventPosition"),
		}
	}

	lastProcessedEventNumber, err := strconv.Atoi(wire.LastCheckpointedEventPosition)

	if err != nil {
		return nil, &Error{
			code: ErrorParsing,
			err:  fmt.Errorf("error when parsing LastCheckpointedEventPosition"),
		}
	}

	startFrom, err := strconv.Atoi(wire.StartFrom)

	if err != nil {
		return nil, &Error{
			code: ErrorParsing,
			err:  fmt.Errorf("error when parsing StartFrom"),
		}
	}

	config := PersistentSubscriptionConfig{
		ResolveLinkTos:       wire.ResolveLinkTos,
		StartFrom:            int64(startFrom),
		MessageTimeout:       int64(wire.MessageTimeoutMilliseconds),
		ExtraStatistics:      wire.ExtraStatistics,
		MaxRetryCount:        int64(wire.MaxRetryCount),
		LiveBufferSize:       int64(wire.LiveBufferSize),
		BufferSize:           int64(wire.BufferSize),
		ReadBatchSize:        int64(wire.ReadBatchSize),
		PreferRoundRobin:     wire.NamedConsumerStrategy == "RoundRobin",
		CheckpointAfter:      int64(wire.CheckPointAfterMilliseconds),
		CheckpointLowerBound: int64(wire.MinCheckPointCount),
		CheckpointUpperBound: int64(wire.MaxCheckPointCount),
		MaxSubscriberCount:   int64(wire.MaxSubscriberCount),
		ConsumerStrategyName: wire.NamedConsumerStrategy,
	}

	var connections []PersistentSubscriptionConnectionInfo
	for _, connWire := range wire.Connections {
		var stats []PersistentSubscriptionMeasurement
		for _, statsWire := range connWire.ObservedMeasurements {
			stats = append(stats, PersistentSubscriptionMeasurement{
				Key:   statsWire.Key,
				Value: statsWire.Value,
			})
		}

		conn := PersistentSubscriptionConnectionInfo{
			From:                      connWire.From,
			Username:                  connWire.Username,
			AverageItemsPerSecond:     float64(connWire.AverageItemsPerSecond),
			TotalItemsProcessed:       connWire.TotalItems,
			CountSinceLastMeasurement: connWire.CountSinceLastMeasurement,
			AvailableSlots:            int64(connWire.AvailableSlots),
			InFlightMessages:          int64(connWire.InFlightMessages),
			ConnectionName:            connWire.ConnectionName,
			ExtraStatistics:           stats,
		}

		connections = append(connections, conn)
	}

	info := PersistentSubscriptionInfoHttpJson{
		EventStreamId:            wire.EventSource,
		GroupName:                wire.GroupName,
		Status:                   wire.Status,
		AverageItemsPerSecond:    float64(wire.AveragePerSecond),
		TotalItemsProcessed:      wire.TotalItems,
		LastProcessedEventNumber: int64(lastProcessedEventNumber),
		LastKnownEventNumber:     int64(lastKnownEventNumber),
		ConnectionCount:          int64(len(wire.Connections)),
		TotalInFlightMessages:    int64(wire.TotalInFlightMessages),
		Config:                   &config,
		Connections:              connections,
		ReadBufferCount:          int64(wire.ReadBufferCount),
		RetryBufferCount:         int64(wire.RetryBufferCount),
		LiveBufferCount:          wire.LiveBufferCount,
		OutstandingMessagesCount: int64(wire.OutstandingMessagesCount),
		ParkedMessageCount:       wire.ParkedMessageCount,
	}

	return &info, nil
}

func newPersistentClient(inner *grpcClient, client persistent.PersistentSubscriptionsClient) persistentClient {
	return persistentClient{
		inner:                        inner,
		persistentSubscriptionClient: client,
	}
}
