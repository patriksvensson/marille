namespace Marille.Tests;

public class ExceptionWorker : IWorker<WorkQueuesEvent> {
	public string Id { get; set; } = string.Empty;

	public ExceptionWorker (string id)
	{
		Id = id;
	}

	public async Task ConsumeAsync (WorkQueuesEvent message, CancellationToken cancellationToken = default)
	{
		await Task.Delay (TimeSpan.FromMilliseconds (10));
		throw new InvalidOperationException ($"Excepiton from worker {Id}");
	}
		
}
