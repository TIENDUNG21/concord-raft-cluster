using Concord.Checker;
using Concord.Core;
using Concord.Kv;

namespace Concord.Simulation;

/// <summary>A client request on the simulated wire. (ClientId, Sequence) make retries idempotent.</summary>
public sealed record ClientRequest(long ClientId, long Sequence, KvOpKind Kind, string Key, string? Value, string? Expected);

/// <summary>The node's answer to <see cref="ClientRequest"/> (same client and sequence).</summary>
public sealed record ClientResponse(long ClientId, long Sequence, KvResponse Response);

/// <summary>A real Concord node in the simulator: <see cref="KvStateMachine"/> plus <see cref="KvService"/>.</summary>
public sealed class KvApplication : INodeApplication
{
    private readonly KvServiceOptions _options;
    private KvService? _service;

    public KvApplication(KvServiceOptions? options = null)
    {
        _options = options ?? new KvServiceOptions();
    }

    public KvStateMachine Machine { get; } = new();

    public IStateMachine StateMachine => Machine;

    public void Attach(RaftNode node) => _service = new KvService(node, Machine, _options);

    public void Receive(int from, object payload, Action<int, object> reply)
    {
        ArgumentNullException.ThrowIfNull(reply);
        if (payload is not ClientRequest request || _service is null)
        {
            return;
        }

        void Answer(KvResponse response) => reply(from, new ClientResponse(request.ClientId, request.Sequence, response));

        switch (request.Kind)
        {
            case KvOpKind.Get:
                _service.Read(request.Key, Answer);
                break;
            case KvOpKind.Put:
                _service.Write(KvCommand.Put(request.ClientId, request.Sequence, request.Key, request.Value!), Answer);
                break;
            case KvOpKind.Delete:
                _service.Write(KvCommand.Delete(request.ClientId, request.Sequence, request.Key), Answer);
                break;
            case KvOpKind.CompareAndSet:
                _service.Write(KvCommand.CompareAndSet(request.ClientId, request.Sequence, request.Key, request.Expected, request.Value!), Answer);
                break;
        }
    }
}
