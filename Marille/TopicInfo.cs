using System.Threading.Channels;

namespace Marille;

internal record TopicInfo<T> (TopicConfiguration Configuration, Channel<Message<T>> Channel, 
	Channel<WorkerError> ErrorChannel) where T : struct;
