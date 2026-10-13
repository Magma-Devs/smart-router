package chainlib

import (
	"context"
	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils"
)

func recordCustomerRejection(logger *metrics.RPCConsumerLogs, request *fiber.Ctx, chain, apiInterface string, err error) {
	headers := detachedReqHeaders(request)
	ctx := utils.ExtractWantedHeadersFromCachedMap(headers, context.Background())
	data := metrics.NewRelayAnalytics(extractDappIDFromFiberContext(request), chain, apiInterface)
	logger.BeginCustomerRequest(ctx, data)
	logger.AddMetricForHttp(data, err, headers)
}
