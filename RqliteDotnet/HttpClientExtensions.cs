using System.Text.Json;

namespace RqliteDotnet;

public static class HttpClientExtensions
{
    public static async Task<T> SendTyped<T>(this HttpClient client, HttpRequestMessage request)
    {
        var response = await client.SendAsync(request);
        var content = await response.Content.ReadAsStringAsync();

        // TODO: Fix this because it bombs on a lot of different scenarios that don't match exactly
        var result = JsonSerializer.Deserialize<T>(content, new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });
        return result;
    }
}