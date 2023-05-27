using System.Data;
using System.Diagnostics;
using System.Net;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Text.Json;
using RqliteDotnet.Dto;

namespace RqliteDotnet;

public class RqliteClient
{
    private readonly HttpClient _httpClient;

    public RqliteClient(string uri, HttpClient? client = null, int HTTPClientTimeout = 300)
    {
        _httpClient = client ?? new HttpClient(){ BaseAddress = new Uri(uri) };
        _httpClient.Timeout = new TimeSpan(0, 0, HTTPClientTimeout);
    }
    
    /// <summary>
    /// Ping Rqlite instance
    /// </summary>
    /// <returns>String containining Rqlite version</returns>
    public string Ping()
    {
        var response = _httpClient.GetAsync("/status").Result;
        try
        {
            return response.Headers.GetValues("X-Rqlite-Version").FirstOrDefault()!.ToString();
        }
        catch
        {
            return "";
        }
    }

    /// <summary>
    /// Query DB and return result
    /// </summary>
    /// <param name="query"></param>
    public QueryResults? Query(string query, DbFlag? flags = DbFlag.Timings)
    {
        var data = "&q=" + Uri.EscapeDataString(query);
        var baseUrl = GetPath("query", flags); //"/db/query?timings";

        var response = _httpClient.GetAsync($"{baseUrl}&{data}").Result.Content.ToString() ?? "";        
        var result = JsonSerializer.Deserialize<QueryResults>(response, new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });
        return result;
    }

    /// <summary>
    /// QueryAsync DB and return result
    /// </summary>
    /// <param name="query"></param>
    public async Task<QueryResults?> QueryAsync(string query, DbFlag? flags = DbFlag.None)
    {
        var data = "&q="+Uri.EscapeDataString(query);
        var baseUrl = GetPath("query", flags); //"/db/query?timings";

        var r = await _httpClient.GetAsync($"{baseUrl}&{data}");
        var str = await r.Content.ReadAsStringAsync();

        var result = JsonSerializer.Deserialize<QueryResults>(str, new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });
        return result;
    }

    /// <summary>
    /// ExecuteAsync command and return result
    /// </summary>
    public async Task<ExecuteResults?> ExecuteAsync(string command, DbFlag? flags = DbFlag.None)
    {
        //var path = new StringBuilder();
        //path.Append("/db/execute?");

        //List<string> flags = new List<string>();

        //if(Timings)     
        //    flags.Add("timings");
        //if (Queue)
        //    flags.Add("queue");
        //string flagsString = string.Join("&", flags);
        //var parameters = GetParameters(flags);

        //path.Append(GetParameters(flags));

        //var path = GetPath("execute", flags);

        //var request = new HttpRequestMessage(HttpMethod.Post, "/db/execute?timings");
        //var request = new HttpRequestMessage(HttpMethod.Post, path.ToString());
        var request = new HttpRequestMessage(HttpMethod.Post, GetPath("execute", flags));
        request.Content = new StringContent($"[\"{command}\"]", Encoding.UTF8, "application/json");

        var result = await _httpClient.SendTypedAsync<ExecuteResults>(request);
        return result;
    }

    private string GetPath(string basePath, DbFlag? flags = DbFlag.None)
    {
        var path = new StringBuilder();
        path.AppendFormat("/db/{0}?",basePath);        
        path.Append(GetParameters(flags));
        return path.ToString();
    }

    /// <summary>
    /// ExecuteAsync command and return result
    /// </summary>
    //public async Task<ExecuteResults> ExecuteAsync(List<string> commands, DbFlag? flags = DbFlag.None)
    //{        
    //    var request = new HttpRequestMessage(HttpMethod.Post, GetPath("execute", flags));

    //    StringBuilder commandSB = new StringBuilder();
    //    commandSB.Append("[");
    //    foreach(string command in commands)
    //    {
    //        commandSB.Append($"\"{command}\",");
    //    }
    //    commandSB.Append("]");

    //    var commandString = commandSB.ToString().Replace(",]", "]");
    //    request.Content = new StringContent(commandString, Encoding.UTF8, "application/json");

    //    ExecuteResults result = await _httpClient.SendTypedAsync<ExecuteResults>(request);
    //    return result;
    //}

    /// <summary>
    /// ExecuteAsync one or several commands and return result
    /// </summary>
    /// <param name="commands">Commands to execute</param>
    /// <param name="flags">Command flags, e.g. whether to use transaction</param>
    /// <returns></returns>
    public async Task<ExecuteResults?> ExecuteAsync(IEnumerable<string> commands, DbFlag? flags = DbFlag.None)
    {

        Stopwatch sw = new Stopwatch();

        sw.Reset();
        sw.Start();

        var request = new HttpRequestMessage(HttpMethod.Post, GetPath("execute",flags));
        Console.WriteLine($"request: {sw.ElapsedMilliseconds}");
        commands = commands.Select(c => $"\"{c}\"");
        Console.WriteLine($"commands: {sw.ElapsedMilliseconds}");
        var s = string.Join(",", commands);
        Console.WriteLine($"string.Join: {sw.ElapsedMilliseconds}");
        request.Content = new StringContent($"[{s}]", Encoding.UTF8, "application/json");
        Console.WriteLine($"request.Content : {sw.ElapsedMilliseconds}");
        var result = await _httpClient.SendTypedAsync<ExecuteResults>(request);
        Console.WriteLine($"result : {sw.ElapsedMilliseconds}");
        return result;
    }

    public ExecuteResults? Execute(IEnumerable<string> commands, DbFlag? flags = DbFlag.None)
    {

        Stopwatch sw = new Stopwatch();

        sw.Reset();
        sw.Start();

        var request = new HttpRequestMessage(HttpMethod.Post, GetPath("execute", flags));
        Console.WriteLine($"request: {sw.ElapsedMilliseconds}");
        commands = commands.Select(c => $"\"{c}\"");
        Console.WriteLine($"commands: {sw.ElapsedMilliseconds}");
        var s = string.Join(",", commands);
        Console.WriteLine($"string.Join: {sw.ElapsedMilliseconds}");
        request.Content = new StringContent($"[{s}]", Encoding.UTF8, "application/json");
        Console.WriteLine($"request.Content : {sw.ElapsedMilliseconds}");
        var result = _httpClient.SendTyped<ExecuteResults>(request);
        Console.WriteLine($"result : {sw.ElapsedMilliseconds}");
        return result;
    }

    /// <summary>
    /// QueryAsync DB using parametrized statement
    /// </summary>
    /// <param name="query"></param>
    /// <param name="qps"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public async Task<QueryResults?> QueryParams<T>(string query, params T[] qps) where T: QueryParameter
    {
        var request = new HttpRequestMessage(HttpMethod.Post, GetPath("query",DbFlag.Timings));
        var sb = new StringBuilder(typeof(T) == typeof(NamedQueryParameter) ?
            $"[[\"{query}\",{{" :
            $"[[\"{query}\",");

        foreach (var qp in qps)
        {
            sb.Append(qp.ToParamString()+",");
        }

        sb.Length -= 1;
        sb.Append(typeof(T) == typeof(NamedQueryParameter) ? "}]]" : "]]");

        request.Content = new StringContent(sb.ToString(), Encoding.UTF8, "application/json");
        var result = await _httpClient.SendTypedAsync<QueryResults>(request);
        return result;
    }

    private string GetParameters(DbFlag? flags = DbFlag.None)
    {
        var result = new StringBuilder();
        if (flags == null) return "";        
        if(flags.HasFlag(DbFlag.None)) return "";

        // If Queue is set then only Queue is used
        if (flags.HasFlag(DbFlag.Queue)) flags = DbFlag.Queue;

        if (flags.HasFlag(DbFlag.Queue)) result.Append("&queue");                    
        if (flags.HasFlag(DbFlag.Timings)) result.Append("&timings");
        if (flags.HasFlag(DbFlag.Transaction)) result.Append("&transaction");
        if (result.Length > 0) result[0] = '?';
        return result.ToString();
    }

    protected object GetValue(string valType, JsonElement el)
    {
        if (el.ValueKind == JsonValueKind.Null) { return null; }

        object? x = valType.ToLower() switch
        {
            "text" => el.GetString(),
            "integer" or "numeric" => el.GetInt64(),
            "int" => el.GetInt32(),
            "real" => el.GetDouble(),
            "timestamp" => el.GetDateTime(),
            "bool" => el.GetInt16(),
            _ => throw new ArgumentException($"Unsupported type '{valType.ToLower()}'")
        };

        return x;
    }
}
