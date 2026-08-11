#pragma once

#include <cstdint>
#include <string>
#include <queue>
#include <mutex>
#include <shared_mutex>
#include <boost/asio.hpp>

#include "sdk/smsdk_ext.h"
#include "Define.h"

class SocketHandler;

template <class SocketType>
class Socket {
public:
	Socket(SM_SocketType st, typename SocketType::socket *asioSocket = nullptr);
	~Socket();

	bool IsOpen();

	bool Bind(const char *hostname, uint16_t port, bool async = true);
	bool Connect(const char *hostname, uint16_t port, bool async = true);
	bool Disconnect();
	bool Listen();
	bool Send(const std::string &data, bool async = true);
	bool SendTo(const std::string &data, const char *hostname, uint16_t port, bool async = true);
	bool SetOption(SM_SocketOption so, int value, bool lock = true);

	IPluginFunction *connectCallback;
	IPluginFunction *incomingCallback;
	IPluginFunction *receiveCallback;
	IPluginFunction *sendqueueEmptyCallback;
	IPluginFunction *disconnectCallback;
	IPluginFunction *errorCallback;

	int32_t smHandle;
	int32_t smCallbackArg;
	volatile unsigned int sendQueueLength;

private:
	void ReceiveHandler(char *buf, size_t bufferSize, size_t bytes, const boost::system::error_code &, std::shared_lock<std::shared_mutex> *);

	void BindPostResolveHandler(typename SocketType::resolver *, typename SocketType::resolver::iterator, const boost::system::error_code &, std::shared_lock<std::shared_mutex> *);

	void ConnectPostResolveHandler(typename SocketType::resolver *, typename SocketType::resolver::iterator, const boost::system::error_code &, std::shared_lock<std::shared_mutex> *);
	void ConnectPostConnectHandler(typename SocketType::resolver *, typename SocketType::resolver::iterator, const boost::system::error_code &, std::shared_lock<std::shared_mutex> *);

	void ListenIncomingHandler(boost::asio::ip::tcp::socket *newAsioSocket, const boost::system::error_code &, std::shared_lock<std::shared_mutex> *);

	void SendPostSendHandler(char *buf, size_t bytes, const boost::system::error_code &err, std::shared_lock<std::shared_mutex> *);

	void SendToPostResolveHandler(typename SocketType::resolver *, typename SocketType::resolver::iterator, char *buf, size_t bufLen, const boost::system::error_code &, std::shared_lock<std::shared_mutex> *);
	void SendToPostSendHandler(typename SocketType::resolver *, typename SocketType::resolver::iterator, char *buf, size_t bufLen, size_t bytesTransferred, const boost::system::error_code &, std::shared_lock<std::shared_mutex> *);

	//void InitializeResolver();
	void InitializeSocket();

	SM_SocketType sm_sockettype;
	std::queue<SocketOption *> socketOptionQueue;

	typename SocketType::socket *socket;
	std::mutex socketMutex;
	//typename SocketType::resolver* resolver;
	typename SocketType::endpoint *localEndpoint;
	std::mutex *localEndpointMutex;
	boost::asio::ip::tcp::acceptor *tcpAcceptor;
	std::mutex *tcpAcceptorMutex;

	std::shared_mutex handlerMutex;
};
