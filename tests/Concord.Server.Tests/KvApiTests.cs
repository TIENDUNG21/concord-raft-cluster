using System.Net;
using System.Net.Http.Json;
using System.Text;
using Concord.Core;
using Concord.Kv;
using Concord.Server.Api;

namespace Concord.Server.Tests;

public sealed class SharedCluster : IAsyncLifetime
{
    public InMemoryCluster Cluster { get; } = new();

    public int Leader { get; private set; }

    public int Follower => Leader % Cluster.Size + 1;

    public async Task InitializeAsync() => Leader = await Cluster.WaitForLeader();

    public async Task DisposeAsync() => await Cluster.DisposeAsync();
}

public sealed class KvApiTests(SharedCluster shared) : IClassFixture<SharedCluster>
{
    private readonly InMemoryCluster _cluster = shared.Cluster;

    private HttpClient Leader => _cluster.Client(shared.Leader);

    private static StringContent Text(string value) => new(value, Encoding.UTF8, "text/plain");

    private static async Task<T> Body<T>(HttpResponseMessage response) =>
        (await response.Content.ReadFromJsonAsync<T>(InMemoryCluster.Json))!;

    [Fact]
    public async Task Status_ShowsLeaderAndMembersOnEveryNode()
    {
        for (var id = 1; id <= 3; id++)
        {
            var status = await _cluster.Status(id);

            Assert.Equal(id, status.NodeId);
            Assert.Equal(shared.Leader, status.LeaderId);
            Assert.Equal($"http://n{shared.Leader}.test/", status.LeaderUrl);
            Assert.Equal(3, status.Members.Count);
        }

        var leader = await _cluster.Status(shared.Leader);
        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.Equal(2, leader.Replication.Count);
    }

    [Fact]
    public async Task PutThenGet_ReturnsTheValue()
    {
        var put = await Leader.PutAsync("/kv/city", Text("Hà Nội"));
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        var written = await Body<WriteResponse>(put);

        var get = await Leader.GetAsync("/kv/city");

        Assert.Equal(HttpStatusCode.OK, get.StatusCode);
        var value = await Body<ValueResponse>(get);
        Assert.Equal("Hà Nội", value.Value);
        Assert.True(value.Index >= written.Index);
    }

    [Fact]
    public async Task Follower_RedirectsToTheLeaderPreservingTheMethod()
    {
        var response = await _cluster.Client(shared.Follower).PutAsync("/kv/redirected?x=1", Text("v"));

        Assert.Equal(HttpStatusCode.TemporaryRedirect, response.StatusCode);
        Assert.Equal(new Uri($"http://n{shared.Leader}.test/kv/redirected?x=1"), response.Headers.Location);
        Assert.Equal(shared.Leader.ToString(System.Globalization.CultureInfo.InvariantCulture), response.Headers.GetValues("Concord-Leader").Single());
    }

    [Fact]
    public async Task Delete_ReportsWhetherTheKeyExisted()
    {
        await Leader.PutAsync("/kv/temp", Text("x"));

        var first = await Body<DeleteResponse>(await Leader.DeleteAsync("/kv/temp"));
        var second = await Body<DeleteResponse>(await Leader.DeleteAsync("/kv/temp"));
        var get = await Leader.GetAsync("/kv/temp");

        Assert.True(first.Deleted);
        Assert.False(second.Deleted);
        Assert.Equal(HttpStatusCode.NotFound, get.StatusCode);
    }

    [Fact]
    public async Task CompareAndSet_SwapsOnceThenConflicts()
    {
        var first = await Leader.PostAsJsonAsync("/kv/lock/cas", new CasRequest(null, "owner-a"));
        var second = await Leader.PostAsJsonAsync("/kv/lock/cas", new CasRequest(null, "owner-b"));

        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        var conflict = await Body<CasResponse>(second);
        Assert.False(conflict.Swapped);
        Assert.Equal("owner-a", conflict.Value);
    }

    [Fact]
    public async Task RetriedWriteWithSameSequence_IsAppliedOnce()
    {
        async Task<HttpResponseMessage> Cas()
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, "/kv/once/cas") { Content = JsonContent.Create(new CasRequest(null, "first")) };
            request.Headers.Add("Concord-Client-Id", "77");
            request.Headers.Add("Concord-Sequence", "1");
            return await Leader.SendAsync(request);
        }

        var original = await Cas();
        var retry = await Cas();

        Assert.Equal(HttpStatusCode.OK, original.StatusCode);
        Assert.Equal(HttpStatusCode.OK, retry.StatusCode);
        Assert.True((await Body<CasResponse>(retry)).Swapped);
    }

    [Theory]
    [InlineData("Concord-Client-Id", "5", null, null)]
    [InlineData("Concord-Client-Id", "abc", "Concord-Sequence", "1")]
    [InlineData("Concord-Client-Id", "0", "Concord-Sequence", "1")]
    public async Task MalformedSessionHeaders_AreRejected(string header1, string value1, string? header2, string? value2)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, "/kv/k") { Content = Text("v") };
        request.Headers.Add(header1, value1);
        if (header2 is not null)
        {
            request.Headers.Add(header2, value2);
        }

        var response = await Leader.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task InvalidInput_GetsClearErrors()
    {
        var longKey = await Leader.GetAsync("/kv/" + new string('k', KvLimits.MaxKeyLength + 1));
        var huge = await Leader.PutAsync("/kv/big", Text(new string('v', KvLimits.MaxValueBytes + 1)));
        var badUtf8 = await Leader.PutAsync("/kv/bytes", new ByteArrayContent(new byte[] { 0xC3, 0x28 }));
        var casWithoutValue = await Leader.PostAsJsonAsync("/kv/x/cas", new CasRequest("a", null));
        var casNotJson = await Leader.PostAsync("/kv/x/cas", new StringContent("{not json", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, longKey.StatusCode);
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, huge.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, badUtf8.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, casWithoutValue.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, casNotJson.StatusCode);
    }

    [Fact]
    public async Task GarbageOnTheRaftEndpoint_IsRejected()
    {
        var response = await Leader.PostAsync("/raft/messages", new ByteArrayContent(new byte[] { 1, 2, 3 }));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("magic", (await Body<ErrorResponse>(response)).Error, StringComparison.Ordinal);
    }
}
