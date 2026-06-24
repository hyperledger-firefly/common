// Copyright © 2024 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package i18n

var (
	FilterJSONCaseInsensitive    = ffm("FilterJSON.caseInsensitive", "Configures whether the comparison is case sensitive - not supported for all operators")
	FilterJSONNot                = ffm("FilterJSON.not", "Negates the comparison operation, so 'equal' becomes 'not equal' for example - not supported for all operators")
	FilterJSONField              = ffm("FilterJSON.field", "Name of the field for the comparison operation")
	FilterJSONValue              = ffm("FilterJSON.value", "A JSON simple value to use in the comparison - must be a string, number or boolean and be parsable for the type of the filter field")
	FilterJSONValues             = ffm("FilterJSON.values", "Array of values to use in the comparison")
	FilterJSONContains           = ffm("FilterJSON.contains", "Array of field + value combinations to apply as string-contains filters - all filters must match")
	FilterJSONEqual              = ffm("FilterJSON.equal", "Array of field + value combinations to apply as equal filters - all must match")
	FilterJSONEq                 = ffm("FilterJSON.eq", "Shortname for equal")
	FilterJSONNEq                = ffm("FilterJSON.neq", "Shortcut for equal with all conditions negated (the not property of all children is overridden)")
	FilterJSONStartsWith         = ffm("FilterJSON.startsWith", "Array of field + value combinations to apply as starts-with filters - all filters must match")
	FilterJSONEndsWith           = ffm("FilterJSON.endsWith", "Array of field + value combinations to apply as ends-with filters - all filters must match")
	FilterJSONGreaterThan        = ffm("FilterJSON.greaterThan", "Array of field + value combinations to apply as greater-than filters - all filters must match")
	FilterJSONGT                 = ffm("FilterJSON.gt", "Short name for greaterThan")
	FilterJSONGreaterThanOrEqual = ffm("FilterJSON.greaterThanOrEqual", "Array of field + value combinations to apply as greater-than filters - all filters must match")
	FilterJSONGTE                = ffm("FilterJSON.gte", "Short name for greaterThanOrEqual")
	FilterJSONLessThan           = ffm("FilterJSON.lessThan", "Array of field + value combinations to apply as less-than-or-equal filters - all filters must match")
	FilterJSONLT                 = ffm("FilterJSON.lt", "Short name for lessThan")
	FilterJSONLessThanOrEqual    = ffm("FilterJSON.lessThanOrEqual", "Array of field + value combinations to apply as less-than-or-equal filters - all filters must match")
	FilterJSONLTE                = ffm("FilterJSON.lte", "Short name for lessThanOrEqual")
	FilterJSONIn                 = ffm("FilterJSON.in", "Array of field + values-array combinations to apply as 'in' filters (matching one of a set of values) - all filters must match")
	FilterJSONNIn                = ffm("FilterJSON.nin", "Shortcut for in with all conditions negated (the not property of all children is overridden)")
	FilterJSONNull               = ffm("FilterJSON.null", "Tests if the specified field is null (unset)")
	FilterJSONLimit              = ffm("FilterJSON.limit", "Limit on the results to return")
	FilterJSONSkip               = ffm("FilterJSON.skip", "Number of results to skip before returning entries, for skip+limit based pagination")
	FilterJSONSort               = ffm("FilterJSON.sort", "Array of fields to sort by. A '-' prefix on a field requests that field is sorted in descending order")
	FilterJSONCount              = ffm("FilterJSON.count", "If true, the total number of entries that could be returned from the database will be calculated and returned as a 'total' (has a performance cost)")
	FilterJSONOr                 = ffm("FilterJSON.or", "Array of sub-queries")  // Note due to complex issue in swagger generator AND/OR need to be identical
	FilterJSONAnd                = ffm("FilterJSON.and", "Array of sub-queries") // ^^^ the `description` field is getting pushed to the sub-schema definition, and is non-deterministic
	FilterJSONFields             = ffm("FilterJSON.fields", "Fields to return in the response")

	EventStreamBatchSize         = ffm("eventstream.batchSize", "Maximum number of events to deliver in each batch")
	EventStreamBatchTimeout      = ffm("eventstream.batchTimeout", "Amount of time to wait for events to arrive before delivering an incomplete batch")
	EventStreamBlockedRetryDelay = ffm("eventstream.blockedRetryDelay", "Amount of time to wait between retries, when a stream is blocked")
	EventStreamConfig            = ffm("eventstream.config", "Additional configuration for the event stream")
	EventStreamCreated           = ffm("eventstream.created", "Time the event stream was created")
	EventStreamUpdated           = ffm("eventstream.updated", "Time the event stream was last updated")
	EventStreamErrorHandling     = ffm("eventstream.errorHandling", "When an error is encountered, and short retries are exhausted, whether to skip the event or block the stream (default=block)")
	EventStreamID                = ffm("eventstream.id", "ID of the event stream")
	EventStreamInitialSequenceID = ffm("eventstream.initialSequenceId", "Initial sequence ID to begin event delivery from")
	EventStreamName              = ffm("eventstream.name", "Unique name for the event stream")
	EventStreamRetryTimeout      = ffm("eventstream.retryTimeout", "Short retry timeout before error handling, in case of webhook based delivery")
	EventStreamStatus            = ffm("eventstream.status", "Status information for the event stream")
	EventStreamTopicFilter       = ffm("eventstream.topicFilter", "Regular expression to apply to the topic of each event for server-side filtering")
	EventStreamType              = ffm("eventstream.type", "Delivery type for the event stream")
	EventStreamWebHook           = ffm("eventstream.webhook", "Webhook configuration options")
	EventStreamWebSocket         = ffm("eventstream.websocket", "WebSocket configuration options")
	EventStreamStatistics        = ffm("EventStream.statistics", "Information about the status and operation of the event stream")
	EventStreamStatusExt         = ffm("EventStream.status", "Status of the event stream")

	EventStreamWHConfigHeaders = ffm("whconfig.headers", "Headers to add to the HTTP call")
	EventStreamWHConfigHTTP    = ffm("whconfig.http", "Base client config for the Webhook HTTP requests")
	EventStreamWHConfigMethod  = ffm("whconfig.method", "HTTP method to use for the HTTP requests")
	EventStreamWHTLSConfigName = ffm("whconfig.tlsConfigName", "Name of a TLS configuration to use for this Webhook")
	EventStreamWHURL           = ffm("whconfig.url", "URL to invoke")

	EventStreamWSDistributionMode = ffm("wsconfig.distributionMode", "Whether to 'broadcast' messages (at most once), or 'load_balance' requests between connections with acknowledgement (at least once)")

	EventStreamStatisticsStartTime            = ffm("EventStreamStatistics.startTime", "Time the stream started")
	EventStreamStatisticsLastDispatchTime     = ffm("EventStreamStatistics.lastDispatchTime", "Time the stream last dispatched a message")
	EventStreamStatisticsLastDispatchBatch    = ffm("EventStreamStatistics.lastDispatchBatch", "Batch number of the last dispatched batch")
	EventStreamStatisticsLastDispatchAttempts = ffm("EventStreamStatistics.lastDispatchAttempts", "Number of attempts to dispatch the current batch")
	EventStreamStatisticsLastDispatchFailure  = ffm("EventStreamStatistics.lastDispatchFailure", "Error message for the last failure that occurred")
	EventStreamStatisticsLastDispatchComplete = ffm("EventStreamStatistics.lastDispatchComplete", "Completion status of the last batch")
	EventStreamStatisticsHighestDetected      = ffm("EventStreamStatistics.highestDetected", "Highest sequence ID detected")
	EventStreamStatisticsHighestDispatched    = ffm("EventStreamStatistics.highestDispatched", "Highest sequence ID dispatched")
	EventStreamStatisticsCheckpoint           = ffm("EventStreamStatistics.checkpoint", "Current checkpoint sequence ID")

	RESTConfigAuthPassword                  = ffm("RESTConfig.authPassword", "Password for the HTTP/HTTPS Basic Auth header")
	RESTConfigAuthUsername                  = ffm("RESTConfig.authUsername", "Username for the HTTP/HTTPS Basic Auth header")
	RESTConfigConnectionTimeout             = ffm("RESTConfig.connectionTimeout", "HTTP connection timeout")
	RESTConfigDNSServers                    = ffm("RESTConfig.dnsServers", "An optional list of DNS server addresses (host or host:port, port defaults to 53) to use for name resolution. Setting this forces use of Go's built-in DNS resolver rather than the system resolver")
	RESTConfigExpectContinueTimeout         = ffm("RESTConfig.expectContinueTimeout", "Time to wait for the first response from the server after connecting")
	RESTConfigExpectHeaders                 = ffm("RESTConfig.headers", "Headers to add to the HTTP call")
	RESTConfigHTTPPassthroughHeadersEnabled = ffm("RESTConfig.httpPassthroughHeadersEnabled", "Proxy request ID or other configured headers from an upstream microservice connection")
	RESTConfigIdleTimeout                   = ffm("RESTConfig.idleTimeout", "Time to leave idle connections in the connection pool")
	RESTConfigThrottleRequestsPerSecond     = ffm("RESTConfig.requestsPerSecond", "Requests per second")
	RESTConfigThrottleBurst                 = ffm("RESTConfig.burst", "Burst")
	RESTConfigMaxConnsPerHost               = ffm("RESTConfig.maxConnsPerHost", "Maximum connections per host")
	RESTConfigMaxIdleConnsPerHost           = ffm("RESTConfig.maxIdleConnsPerHost", "Maximum idle connections per host")
	RESTConfigMaxIdleConns                  = ffm("RESTConfig.maxIdleConns", "Maximum idle connections to leave in the connection pool")
	RESTConfigMaxIdleTimeout                = ffm("RESTConfig.maxIdleTimeout", "Maximum time to leave idle connections in the connection pool")
	RESTConfigProxyURL                      = ffm("RESTConfig.proxyURL", "URL of a proxy server to use for connections")
	RESTConfigRequestTimeout                = ffm("RESTConfig.requestTimeout", "Maximum time any individual request can take")
	RESTConfigRetry                         = ffm("RESTConfig.retry", "Whether to automatically retry failed HTTP requests")
	RESTConfigRetryCount                    = ffm("RESTConfig.retryCount", "Maximum number of times to retry")
	RESTConfigRetryErrorStatusCodeRegex     = ffm("RESTConfig.retryErrorStatusCodeRegex", "Regular expression to apply to the status codes of failed request to determine whether to retry")
	RESTConfigRetryInitialDelay             = ffm("RESTConfig.retryInitialDelay", "Time to wait before the first retry")
	RESTConfigRetryMaximumDelay             = ffm("RESTConfig.retryMaximumDelay", "Maximum time to wait between retries")
	RESTConfigTLSHandshakeTimeout           = ffm("RESTConfig.tlsHandshakeTimeout", "Maximum time to wait for the TLS handshake to complete")

	CollectionResultsCount = ffm("CollectionResults.count", "Number of items returned from this call")
	CollectionResultsTotal = ffm("CollectionResults.total", "Number of items total that could be returned, if a count was requested")
	CollectionResultsItems = ffm("CollectionResults.items", "The array of items")
)
