namespace BigFileSorter.GeneralComponents
{
    internal interface IDataSource<T> : IDisposable
    {
        (bool isEmpty, T line) Next();
    }
}