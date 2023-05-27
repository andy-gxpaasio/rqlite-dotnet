namespace RqliteDotnet.Dto;

public class QueryResults
{
    public List<QueryResult>? Results { get; set; }

    public bool HasError => Results?.Any(x => x.Error != null) ?? false;
    public int Count => Results?.Sum(x => x.Count) ?? 0;
}