using System.Text.Json.Serialization;
using Concord.Core.Storage;
using Concord.Server.Api;
using Concord.Server.Hosting;
using Concord.Server.Storage;
using Concord.Server.Transport;

var builder = WebApplication.CreateBuilder(args);

// Short flags for running a node by hand: --node-id 1 --peers 1=http://...,2=... --data-dir ./data/n1
builder.Configuration.AddCommandLine(args, new Dictionary<string, string>(StringComparer.Ordinal)
{
    ["--node-id"] = "Concord:NodeId",
    ["--peers"] = "Concord:PeerList",
    ["--data-dir"] = "Concord:DataDirectory",
});

builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddSingleton(sp => ConcordSettings.From(sp.GetRequiredService<IConfiguration>()));
builder.Services.AddSingleton<IRaftStorage>(sp => FileRaftStorage.Open(sp.GetRequiredService<ConcordSettings>().DataDirectory));
builder.Services.AddHttpClient(HttpTransport.ClientName, client => client.Timeout = TimeSpan.FromSeconds(2));
builder.Services.AddSingleton<HttpTransport>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<HttpTransport>());
builder.Services.AddSingleton<RaftHost>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RaftHost>());

var app = builder.Build();
app.MapConcordApi();
app.Run();

/// <summary>Entry point; public so integration tests can host it.</summary>
public partial class Program;
