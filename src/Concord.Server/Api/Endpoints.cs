using System.Globalization;
using System.Text;
using Concord.Core;
using Concord.Core.Serialization;
using Concord.Kv;
using Concord.Server.Hosting;
using Microsoft.AspNetCore.Mvc;

namespace Concord.Server.Api;

public sealed record ValueResponse(string Key, string? Value, long Index);

public sealed record WriteResponse(string Key, long Index);

public sealed record DeleteResponse(string Key, bool Deleted, long Index);

public sealed record CasRequest(string? Expected, string? Value);

public sealed record CasResponse(string Key, bool Swapped, string? Value, long Index);

public sealed record ErrorResponse(string Error, int? LeaderId = null);

public sealed record MemberResponse(int Id, string Url);

public sealed record StatusResponse(
    int NodeId,
    RaftRole Role,
    long Term,
    int? LeaderId,
    string? LeaderUrl,
    long CommitIndex,
    long LastApplied,
    long LastLogIndex,
    long SnapshotIndex,
    IReadOnlyList<MemberResponse> Members,
    IReadOnlyList<PeerStatus> Replication);

/// <summary>
/// HTTP API. Any node accepts any request; a follower answers 307 to the leader (curl -L and most
/// clients follow it and resend the body). Optional Concord-Client-Id / Concord-Sequence headers make
/// write retries exactly-once through the state machine's client sessions.
/// </summary>
internal static class Endpoints
{
    public const string ClientIdHeader = "Concord-Client-Id";
    public const string SequenceHeader = "Concord-Sequence";
    public const string LeaderHeader = "Concord-Leader";

    public static void MapConcordApi(this WebApplication app)
    {
        app.MapGet("/healthz", () => Results.Text("ok"));

        app.MapGet("/kv/{key}", async (string key, HttpContext http, RaftHost host, ConcordSettings settings) =>
        {
            if (KvLimits.ValidateKey(key) is { } problem)
            {
                return Results.BadRequest(new ErrorResponse(problem));
            }

            return await Respond(http, settings, host.ReadAsync(key), r => r.Result!.Succeeded
                ? Results.Ok(new ValueResponse(key, r.Result.Value, r.Index))
                : Results.NotFound(new ValueResponse(key, null, r.Index)));
        });

        app.MapPut("/kv/{key}", async (string key, HttpContext http, RaftHost host, ConcordSettings settings) =>
        {
            var body = await ReadValue(http.Request);
            if (body.Error is { } error)
            {
                return error;
            }

            return await Write(http, host, settings, (client, sequence) => KvCommand.Put(client, sequence, key, body.Value!), r => Results.Ok(new WriteResponse(key, r.Index)));
        });

        app.MapDelete("/kv/{key}", (string key, HttpContext http, RaftHost host, ConcordSettings settings) =>
            Write(http, host, settings, (client, sequence) => KvCommand.Delete(client, sequence, key), r => Results.Ok(new DeleteResponse(key, r.Result!.Succeeded, r.Index))));

        app.MapPost("/kv/{key}/cas", (string key, [FromBody] CasRequest request, HttpContext http, RaftHost host, ConcordSettings settings) =>
        {
            if (request.Value is null)
            {
                return Task.FromResult(Results.BadRequest(new ErrorResponse("\"value\" is required; \"expected\": null means the key must be absent")));
            }

            return Write(http, host, settings, (client, sequence) => KvCommand.CompareAndSet(client, sequence, key, request.Expected, request.Value), r => r.Result!.Succeeded
                ? Results.Ok(new CasResponse(key, true, r.Result.Value, r.Index))
                : Results.Conflict(new CasResponse(key, false, r.Result.Value, r.Index)));
        });

        app.MapGet("/cluster/status", async (RaftHost host, ConcordSettings settings) =>
        {
            var status = await host.StatusAsync();
            return Results.Ok(new StatusResponse(
                status.Id,
                status.Role,
                status.Term,
                status.LeaderId,
                settings.UrlOf(status.LeaderId)?.ToString(),
                status.CommitIndex,
                status.LastApplied,
                status.LastLogIndex,
                status.SnapshotIndex,
                settings.PublicUrls.OrderBy(m => m.Key).Select(m => new MemberResponse(m.Key, m.Value.ToString())).ToArray(),
                status.Peers));
        });

        app.MapPost("/raft/messages", async (HttpRequest request, RaftHost host) =>
        {
            using var buffer = new MemoryStream();
            await request.Body.CopyToAsync(buffer);
            try
            {
                host.Deliver(RaftMessageCodec.Decode(buffer.GetBuffer().AsSpan(0, (int)buffer.Length)));
                return Results.NoContent();
            }
            catch (FormatException ex)
            {
                return Results.BadRequest(new ErrorResponse(ex.Message));
            }
        }).WithMetadata(new RequestSizeLimitAttribute(80 << 20));
    }

    private static Task<IResult> Write(
        HttpContext http,
        RaftHost host,
        ConcordSettings settings,
        Func<long, long, KvCommand> build,
        Func<KvResponse, IResult> onSuccess)
    {
        if (!TryReadSession(http.Request, out var client, out var sequence, out var problem))
        {
            return Task.FromResult(Results.BadRequest(new ErrorResponse(problem!)));
        }

        var command = build(client, sequence);
        if (command.Validate() is { } invalid)
        {
            return Task.FromResult(Results.BadRequest(new ErrorResponse(invalid)));
        }

        return Respond(http, settings, host.WriteAsync(command), onSuccess);
    }

    private static async Task<IResult> Respond(HttpContext http, ConcordSettings settings, Task<KvResponse> pending, Func<KvResponse, IResult> onSuccess)
    {
        KvResponse response;
        try
        {
            response = await pending.WaitAsync(settings.RequestTimeout, http.RequestAborted);
        }
        catch (TimeoutException)
        {
            return Results.Json(new ErrorResponse("timed out waiting for the cluster; the write may still be applied"), statusCode: StatusCodes.Status504GatewayTimeout);
        }

        switch (response.Status)
        {
            case KvStatus.Ok:
                return onSuccess(response);
            case KvStatus.Rejected:
                return Results.BadRequest(new ErrorResponse(response.Error ?? "invalid request"));
            case KvStatus.NotLeader when settings.UrlOf(response.LeaderHint) is { } leader && response.LeaderHint != settings.NodeId:
                http.Response.Headers[LeaderHeader] = response.LeaderHint!.Value.ToString(CultureInfo.InvariantCulture);
                var target = new Uri(leader, http.Request.Path.Value!.TrimStart('/') + http.Request.QueryString);
                return Results.Redirect(target.ToString(), permanent: false, preserveMethod: true);
            default:
                http.Response.Headers.RetryAfter = "1";
                var reason = response.Status == KvStatus.Unknown
                    ? "leadership changed before the write committed; retry with the same Concord-Sequence"
                    : "no leader is known yet; retry shortly";
                return Results.Json(new ErrorResponse(reason, response.LeaderHint), statusCode: StatusCodes.Status503ServiceUnavailable);
        }
    }

    private static bool TryReadSession(HttpRequest request, out long client, out long sequence, out string? problem)
    {
        client = 0;
        sequence = 0;
        problem = null;
        var hasClient = request.Headers.TryGetValue(ClientIdHeader, out var clientText);
        var hasSequence = request.Headers.TryGetValue(SequenceHeader, out var sequenceText);
        if (!hasClient && !hasSequence)
        {
            return true;
        }

        if (hasClient != hasSequence
            || !long.TryParse(clientText, NumberStyles.None, CultureInfo.InvariantCulture, out client)
            || !long.TryParse(sequenceText, NumberStyles.None, CultureInfo.InvariantCulture, out sequence)
            || client <= 0
            || sequence <= 0)
        {
            problem = $"{ClientIdHeader} and {SequenceHeader} must be given together as positive integers";
            return false;
        }

        return true;
    }

    private static async Task<(string? Value, IResult? Error)> ReadValue(HttpRequest request)
    {
        if (request.ContentLength > KvLimits.MaxValueBytes)
        {
            return (null, TooLarge());
        }

        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];
        int read;
        while ((read = await request.Body.ReadAsync(chunk)) > 0)
        {
            if (buffer.Length + read > KvLimits.MaxValueBytes)
            {
                return (null, TooLarge());
            }

            buffer.Write(chunk, 0, read);
        }

        try
        {
            return (new UTF8Encoding(false, throwOnInvalidBytes: true).GetString(buffer.GetBuffer(), 0, (int)buffer.Length), null);
        }
        catch (DecoderFallbackException)
        {
            return (null, Results.BadRequest(new ErrorResponse("value is not valid UTF-8")));
        }
    }

    private static IResult TooLarge() =>
        Results.Json(new ErrorResponse($"value larger than {KvLimits.MaxValueBytes} bytes"), statusCode: StatusCodes.Status413PayloadTooLarge);
}
