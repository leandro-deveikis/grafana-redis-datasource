package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"bitbucket.org/creachadair/shell"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	gProxy "github.com/grafana/grafana-plugin-sdk-go/backend/proxy"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/mediocregopher/radix/v4"
	"golang.org/x/net/proxy"

	goredis "github.com/redis/go-redis/v9"
)

/**
 * The function is called when the instance is created for the first time or when a datasource configuration changed.
 */
func newDatasource() datasource.ServeOpts {
	im := datasource.NewInstanceManager(newDataSourceInstance)

	ds := &redisDatasource{
		im: im,
	}

	// Returns datasource.ServeOpts
	return datasource.ServeOpts{
		QueryDataHandler:   ds,
		CheckHealthHandler: ds,
	}
}

/**
* Find element in the slice
 */
func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

/**
 * QueryData handles multiple queries and returns multiple responses.
 * req contains the queries []DataQuery (where each query contains RefID as a unique identifer).
 * The QueryDataResponse contains a map of RefID to the response for each query, and each response contains Frames ([]*Frame).
 */
func (ds *redisDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	log.DefaultLogger.Debug("QueryData", "request", req)

	// Get Instance
	client, err := ds.getInstance(ctx, req.PluginContext)
	if err != nil {
		return nil, err
	}

	// Create response struct
	response := backend.NewQueryDataResponse()

	// Loop over queries and execute them individually
	for _, q := range req.Queries {
		var qm queryModel

		// Unmarshal the json into our queryModel
		err := json.Unmarshal(q.JSON, &qm)
		log.DefaultLogger.Debug("QueryData", "JSON", q.JSON)

		// Error
		if err != nil {
			resp := backend.DataResponse{}
			resp.Error = err
			response.Responses[q.RefID] = resp
			continue
		}

		// Execute query
		resp := query(ctx, q, client, qm)

		// Add Time for Streaming and filter fields
		if qm.Streaming && qm.StreamingDataType != "DataFrame" {
			for _, frame := range resp.Frames {
				timeValues := []time.Time{}

				len, _ := frame.RowLen()
				if len > 0 {
					for j := 0; j < len; j++ {
						timeValues = append(timeValues, time.Now())
					}
				}

				// Filter Fields for Alerting and traffic optimization
				if qm.Field != "" {
					// Split Field to array
					fields, ok := shell.Split(qm.Field)

					// Check if filter is valid
					if !ok {
						resp.Error = fmt.Errorf("field is not valid")
						continue
					}

					filterFields := []*data.Field{}

					// Filter fields
					for _, field := range frame.Fields {
						_, found := Find(fields, field.Name)

						if !found {
							continue
						}
						filterFields = append(filterFields, field)
					}
					frame.Fields = append([]*data.Field{data.NewField("#time", nil, timeValues)}, filterFields...)
				} else {
					frame.Fields = append([]*data.Field{data.NewField("#time", nil, timeValues)}, frame.Fields...)
				}
			}
		}

		// save the response in a hashmap based on with RefID as identifier
		response.Responses[q.RefID] = resp
	}

	return response, nil
}

/**
 * CheckHealth handles health checks sent from Grafana to the plugin
 *
 * @see https://redis.io/commands/ping
 */
func (ds *redisDatasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status backend.HealthStatus
	message := "Data Source health is yet to become known."

	// Get Instance
	client, err := ds.getInstance(ctx, req.PluginContext)

	if err != nil {
		status = backend.HealthStatusError
		message = fmt.Sprintf("getInstance error: %s", err.Error())
	} else {
		err = client.RunCmd(&message, "PING")

		// Check errors
		if err != nil {
			status = backend.HealthStatusError
			message = fmt.Sprintf("PING command failed: %s", err.Error())
		} else {
			status = backend.HealthStatusOk
			message = "Data Source is working as expected."
		}
	}

	// Return Health result
	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

/**
 * Return Instance
 */
func (ds *redisDatasource) getInstance(ctx context.Context, pluginContext backend.PluginContext) (redisClient, error) {
	s, err := ds.im.Get(ctx, pluginContext)

	if err != nil {
		return nil, err
	}

	// Return client
	return s.(*instanceSettings).client, nil
}

/**
 * New Datasource Instance
 *
 * @see https://github.com/mediocregopher/radix
 */
func newDataSourceInstance(_ context.Context, setting backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	// Parse configuration provided by grafana and create configuration for redisClient
	config, err := createRedisClientConfig(setting)
	if err != nil {
		return nil, err
	}

	testRadixV4ClientWithPDC(config)
	testGoRedisV4ClientWithPDC(config)

	// Create radix implementation of redisClient
	client, err := newRadixV3Client(config)
	if err != nil {
		return nil, err
	}

	// Create datasource instance with redisClient inside
	return &instanceSettings{
		client,
	}, nil
}

func testGoRedisV4ClientWithPDC(config redisClientConfiguration) {
	// This is just a PoC to check if PDC can be implemented using go redis (official redis library). this will:
	//- Create a new client with the configured url, and force PDC.
	//- do a GET for the key 'foo'

	ctx := context.Background()
	log.DefaultLogger.Error("--- LND Test GoRedis - Starting go-redis test")

	// grafana set up
	opts := &gProxy.Options{Enabled: true}
	dialSocksProxy, err := gProxy.Cli.NewSecureSocksProxyContextDialer(opts)
	if err != nil {
		log.DefaultLogger.Error("--- LND Test GoRedis- Error building dialSocksProxy", "err", err)
		return
	}

	contextDialer, ok := dialSocksProxy.(proxy.ContextDialer)
	if !ok {
		log.DefaultLogger.Error("--- LND Test GoRedis- Error casting dialSocksProxy")
		return
	}
	//
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     config.URL,
		Password: "", // no password set
		DB:       0,  // use default DB
		Dialer:   contextDialer.DialContext,
	})

	val, err := rdb.Get(ctx, "foo").Result()
	if err != nil {
		log.DefaultLogger.Error("--- LND Test GoRedis - Error getting value", "err", err)
		return
	}
	log.DefaultLogger.Error("--- LND Test GoRedis - Got foo value", "val", val)
}

func testRadixV4ClientWithPDC(config redisClientConfiguration) {
	// This is just a PoC to check if PDC can be implemented using radixV4. this will:
	//- Create a new client with the configured url, and force PDC.
	//- do a GET for the key 'foo'

	log.DefaultLogger.Error("--- LND Test - starting Radix v4 test")

	opts := &gProxy.Options{Enabled: true}
	dialSocksProxy, err := gProxy.Cli.NewSecureSocksProxyContextDialer(opts)
	if err != nil {
		log.DefaultLogger.Error("--- LND Test RadixV4- Error building dialSocksProxy", "err", err)
		return
	}

	contextDialer, ok := dialSocksProxy.(proxy.ContextDialer)
	if !ok {
		log.DefaultLogger.Error("--- LND Test RadixV4- Error casting dialSocksProxy")
		return
	}

	cfg := radix.PoolConfig{
		Dialer: radix.Dialer{
			NetDialer: contextDialer,
		},
	}

	client, err := cfg.New(context.Background(), "tcp", config.URL)
	if err != nil {
		log.DefaultLogger.Error("--- LND Test RadixV4- Error creating client", "err", err)
		return
	}

	var fooValB []byte
	err = client.Do(context.Background(), radix.Cmd(&fooValB, "GET", "foo"))
	if err != nil {
		log.DefaultLogger.Error("--- LND Test RadixV4- Error getting fooVal", "err", err)
		return
	}
	log.DefaultLogger.Error("--- LND Test RadixV4- got foo value", "fooValB(base64) ", fooValB)
}

// Create redisClientConfiguration instance from the grafana settings
func createRedisClientConfig(setting backend.DataSourceInstanceSettings) (redisClientConfiguration, error) {
	var jsonData dataModel

	// Unmarshal Configuration
	var dataError = json.Unmarshal(setting.JSONData, &jsonData)
	if dataError != nil {
		log.DefaultLogger.Error("JSONData", "Error", dataError)
		return redisClientConfiguration{}, dataError
	}

	// Debug
	log.DefaultLogger.Debug("JSONData", "Values", jsonData)

	// Pool size
	poolSize := 5
	if jsonData.PoolSize > 0 {
		poolSize = jsonData.PoolSize
	}

	// Connect, Read and Write Timeout
	timeout := 10
	if jsonData.Timeout > 0 {
		timeout = jsonData.Timeout
	}

	// Ping Interval, disabled by default
	pingInterval := 0
	if jsonData.PingInterval > 0 {
		pingInterval = jsonData.PingInterval
	}

	// Pipeline Window, disabled by default
	pipelineWindow := 0
	if jsonData.PipelineWindow > 0 {
		pipelineWindow = jsonData.PipelineWindow
	}

	// Configuration
	configuration := redisClientConfiguration{
		URL:            setting.URL,
		Timeout:        timeout,
		PoolSize:       poolSize,
		PingInterval:   pingInterval,
		PipelineWindow: pipelineWindow,
		ACL:            jsonData.ACL,
		TLSAuth:        jsonData.TLSAuth,
		TLSSkipVerify:  jsonData.TLSSkipVerify,
		Client:         jsonData.Client,
		SentinelName:   jsonData.SentinelName,
		User:           jsonData.User,
		SentinelUser:   jsonData.SentinelUser,
		SentinelACL:    jsonData.SentinelACL,
	}

	// Secured Data
	var secureData = setting.DecryptedSecureJSONData
	if secureData != nil {
		if secureData["password"] != "" {
			configuration.Password = secureData["password"]
		}
		if secureData["sentinelPassword"] != "" {
			configuration.SentinelPassword = secureData["sentinelPassword"]
		}

		if secureData["tlsCACert"] != "" {
			configuration.TLSCACert = secureData["tlsCACert"]
		}
		if secureData["tlsClientCert"] != "" {
			configuration.TLSClientCert = secureData["tlsClientCert"]
		}
		if secureData["tlsClientKey"] != "" {
			configuration.TLSClientKey = secureData["tlsClientKey"]
		}
	}

	return configuration, nil
}

/**
 * Called before creating a new instance to close Redis connection pool
 */
func (s *instanceSettings) Dispose() {
	s.client.Close()
}
