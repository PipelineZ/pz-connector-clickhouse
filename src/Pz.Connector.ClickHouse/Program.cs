using Pz.Connector.ClickHouse;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new ChConnector(ctx.LoggerFactory)).ConfigureAwait(false);
