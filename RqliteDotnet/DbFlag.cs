using System.Text.Json;

namespace RqliteDotnet;

[Flags]
public enum DbFlag
{
    None = 1,
    Timings = 2,
    Transaction = 3, 
    Queue = 4
}

// https://code-maze.com/csharp-flags-attribute-for-enum/
public static class DbFlagExtensions
{
    public static bool HasFlag(this DbFlag? userType, DbFlag? typeToCompare)
    => (userType & typeToCompare) == typeToCompare;

    public static DbFlag? Add(this DbFlag? userType, params DbFlag?[] typesToAdd)
    {
        foreach (var flag in typesToAdd)
        {
            userType |= flag;
        }
        return userType;
    }
    public static DbFlag? Remove(this DbFlag? userType, params DbFlag?[] typesToRemove)
    {
        foreach (var item in typesToRemove)
        {
            userType &= ~item;
        }
        return userType;
    }
}