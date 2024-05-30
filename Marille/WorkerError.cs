namespace Marille;

public struct WorkerError {
	
	public string TopicName { get; }
	public Type TopicEventType { get; }
	public object Worker { get; }
	public Exception? WorkerException { get; }
	public WorkerError(string topicName, Type type, object worker, Exception? workerException) {
		TopicName = topicName;
		TopicEventType = type;
		Worker = worker;
		WorkerException = workerException;
	}
		
}
