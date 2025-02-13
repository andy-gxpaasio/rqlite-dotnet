using System.Data;

namespace RqliteDotnet;

public class RqliteOrmClient : RqliteClient
{
    public RqliteOrmClient(string uri, HttpClient? client = null) : base(uri, client) {}
    
    /// <summary>
    /// QueryAsync Rqlite DB and return result as an instance of T
    /// </summary>
    /// <param name="query">QueryAsync to execute</param>
    /// <typeparam name="T">Type of result object</typeparam>
    /// <returns></returns>
    public async Task<List<T>> Query<T>(string query) where T: new()
    {
        var response = await QueryAsync(query);
        if (response.Results!.Count > 1)
            throw new DataException("Query returned more than 1 result. At the moment only 1 result supported");
        var res = response.Results[0];
        
        if (!string.IsNullOrEmpty(res.Error))
            throw new InvalidOperationException(res.Error);
        var list = new List<T>();

        if (res.Values is not null)
        {
            for (int i = 0; i < res.Values.Count; i++)
            {
                var dto = new T();

                foreach (var prop in typeof(T).GetProperties())
                {
                    var index = res.Columns.FindIndex(c => c.ToLower() == prop.Name.ToLower());

                    // Only add if the column is found, otherwise leave null
                    if (index > -1)
                    {
                        var val = GetValue(res.Types[index], res.Values[i][index]);
                        prop.SetValue(dto, val);
                    }                    
                }

                list.Add(dto);
            }
        }

        return list;
    }
}