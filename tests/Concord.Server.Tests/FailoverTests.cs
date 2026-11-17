using System.Net;
using System.Net.Http.Json;
using System.Text;
using Concord.Server.Api;

namespace Concord.Server.Tests;

public sealed class FailoverTests
{
    [Fact]
    public async Task LosingTheLeader_ElectsANewOneThatStillHasCommittedData()
    {
        await using var cluster = new InMemoryCluster();
        var leader = await cluster.WaitForLeader();
        var put = await cluster.Client(leader).PutAsync("/kv/survivor", new StringContent("kept", Encoding.UTF8, "text/plain"));
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        var oldTerm = (await cluster.Status(leader)).Term;

        cluster.Disconnect(leader);
        var others = Enumerable.Range(1, 3).Where(id => id != leader).ToArray();
        var next = await cluster.WaitForLeader(others);

        Assert.NotEqual(leader, next);
        var value = await cluster.Client(next).GetFromJsonAsync<ValueResponse>("/kv/survivor", InMemoryCluster.Json);
        Assert.Equal("kept", value!.Value);
        Assert.True((await cluster.Status(next)).Term > oldTerm);
    }
}
