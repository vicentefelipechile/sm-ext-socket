#include "Socket.h"

#include <assert.h>
#include <cstdio>
#include <exception>

#include "Callback.h"
#include "CallbackHandler.h"
#include "SocketHandler.h"

using namespace boost::asio::ip;

template <class SocketType>
Socket<SocketType>::Socket(SM_SocketType st,
						   typename SocketType::socket* asioSocket) : connectCallback(NULL),
																	  incomingCallback(NULL),
																	  receiveCallback(NULL),
																	  sendqueueEmptyCallback(NULL),
																	  disconnectCallback(NULL),
																	  errorCallback(NULL),
																	  smCallbackArg(0),
																	  sendQueueLength(0),
																	  sm_sockettype(st),
																	  socket(NULL),
																	  localEndpoint(NULL),
																	  localEndpointMutex(NULL),
																	  tcpAcceptor(NULL),
																	  tcpAcceptorMutex(NULL) {
	if (asioSocket != NULL) {
		socket = asioSocket;
	}
}

template <class SocketType>
Socket<SocketType>::~Socket() {
	if (socket) {
		std::lock_guard<std::mutex> l(socketMutex);
		socket->close();
		
		delete socket;
		socket = NULL;
	}

	if (tcpAcceptor) {
		std::lock_guard<std::mutex> l(*tcpAcceptorMutex);
		tcpAcceptor->close();
		
		delete tcpAcceptor;
		tcpAcceptor = NULL;
	}
	
	if (localEndpoint) {
		std::lock_guard<std::mutex> l(*localEndpointMutex);
		
		delete localEndpoint;
		localEndpoint = NULL;
	}

	// wait for all callbacks to terminate
	//std::unique_lock<std::shared_mutex> l(handlerMutex);
	handlerMutex.lock();

	std::lock_guard<std::mutex> socketLock(socketMutex);

	if (tcpAcceptorMutex) delete tcpAcceptorMutex;
	if (localEndpointMutex) delete localEndpointMutex;

	while (!socketOptionQueue.empty()) {
		delete socketOptionQueue.front();
		socketOptionQueue.pop();
	}
}

template <class SocketType>
void Socket<SocketType>::ReceiveHandler(char* buf, size_t bufferSize, size_t bytesTransferred, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
	if (!errorCode) {
		std::lock_guard<std::mutex> l(socketMutex);

		if (socket) {
			if (bytesTransferred) callbackHandler.AddCallback(new Callback(CallbackEvent_Receive, this, buf, bytesTransferred));

			socket->async_receive(boost::asio::buffer(buf, bufferSize),
								[this, buf, bufferSize, handlerLock](const boost::system::error_code& ec, size_t bytesTransferred) {
									ReceiveHandler(buf, bufferSize, bytesTransferred, ec, handlerLock);
								});
			return;
		}
	}
	
	if (errorCode) {
		if (errorCode == boost::asio::error::eof ||
			errorCode == boost::asio::error::connection_reset ||
			errorCode == boost::asio::error::connection_aborted) {
			// asio indicates disconnect

			std::lock_guard<std::mutex> l(socketMutex);
			
			if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_Disconnect, this));

		} else if (errorCode != boost::asio::error::operation_aborted) {
			// error

			std::lock_guard<std::mutex> l(socketMutex);
			
			if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_Error, this, SM_ErrorType_RECV_ERROR, errorCode.value()));
			
		}
	}
	
	delete[] buf;
	delete handlerLock;
}

template <class SocketType>
bool Socket<SocketType>::IsOpen() {
	std::lock_guard<std::mutex> l(socketMutex);

	return (socket && socket->is_open());
}

template <class SocketType>
bool Socket<SocketType>::Bind(const char* hostname, uint16_t port, bool async) {
	typename SocketType::resolver* resolver = NULL;
	std::shared_lock<std::shared_mutex>* handlerLock = NULL;

	try {
		if (localEndpoint) {
			// TODO: make sure endpoint is not in use
			//localEndpointInitialized = false;
			//delete localEndpoint;
			return false;
		}

		char sPort[6];
		snprintf(sPort, sizeof(sPort), "%hu", port);

		if (async) {
			resolver = new typename SocketType::resolver(*socketHandler.ioService);
			handlerLock = new std::shared_lock<std::shared_mutex>(handlerMutex);

			resolver->async_resolve(typename SocketType::resolver::query(SocketType::v4(), hostname, sPort),
									[this, resolver, handlerLock](const boost::system::error_code& ec, typename SocketType::resolver::iterator endpointIterator) {
										BindPostResolveHandler(resolver, endpointIterator, ec, handlerLock);
									});
		} else {
			typename SocketType::resolver syncResolver(*socketHandler.ioService);

			typename SocketType::resolver::iterator endpointIterator = syncResolver.resolve(typename SocketType::resolver::query(SocketType::v4(), hostname, sPort));

			if (!localEndpoint) {
				localEndpointMutex = new std::mutex();
				std::lock_guard<std::mutex> l(*localEndpointMutex);
				localEndpoint = new typename SocketType::endpoint(endpointIterator->endpoint());
			}
		}

		return true;
	} catch (std::exception&) {
		if (resolver) delete resolver;
		if (handlerLock) delete handlerLock;
	}

	return false;
}

template <class SocketType>
void Socket<SocketType>::BindPostResolveHandler(typename SocketType::resolver* resolver, typename SocketType::resolver::iterator endpointIterator, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
	if (!errorCode) {
		if (!localEndpoint) {
			localEndpointMutex = new std::mutex();
			std::lock_guard<std::mutex> l(*localEndpointMutex);
			localEndpoint = new typename SocketType::endpoint(*endpointIterator);
		}
	} else if (errorCode != boost::asio::error::operation_aborted) {
		callbackHandler.AddCallback(new Callback(CallbackEvent_Error, this, SM_ErrorType_BIND_ERROR, errorCode.value()));
	}

	delete resolver;
	delete handlerLock;
}

template <class SocketType>
bool Socket<SocketType>::Connect(const char* hostname, uint16_t port, bool async) {
	typename SocketType::resolver* resolver = NULL;
	std::shared_lock<std::shared_mutex>* handlerLock = NULL;

	try {
		char sPort[6];
		snprintf(sPort, sizeof(sPort), "%hu", port);

		if (!socket) InitializeSocket();

		if (async) {
			resolver = new typename SocketType::resolver(*socketHandler.ioService);
			handlerLock = new std::shared_lock<std::shared_mutex>(handlerMutex);

			resolver->async_resolve(typename SocketType::resolver::query(SocketType::v4(), hostname, sPort),
									[this, resolver, handlerLock](const boost::system::error_code& ec, typename SocketType::resolver::iterator endpointIterator) {
										ConnectPostResolveHandler(resolver, endpointIterator, ec, handlerLock);
									});
		} else {
			typename SocketType::resolver syncResolver(*socketHandler.ioService);

			typename SocketType::resolver::iterator endpointIterator = syncResolver.resolve(typename SocketType::resolver::query(SocketType::v4(), hostname, sPort));

			boost::system::error_code error = boost::asio::error::host_not_found;
			typename SocketType::resolver::iterator end;

			while (error && endpointIterator != end) {
				std::lock_guard<std::mutex> l(socketMutex);

				if (socket) {
					socket->connect(*endpointIterator++, error);
					if (error) socket->close();
				} else {
					throw std::logic_error("Operation cancelled.");
				}
			}

			if (error) throw boost::system::system_error(error);

			ReceiveHandler(new char[16384], 16384, 0, boost::system::errc::make_error_code(boost::system::errc::success), new std::shared_lock<std::shared_mutex>(handlerMutex));
		}

		return true;
	} catch (std::exception&) {
		if (resolver) delete resolver;
		if (handlerLock) delete handlerLock;
	}

	return false;
}

template <class SocketType>
void Socket<SocketType>::ConnectPostResolveHandler(typename SocketType::resolver* resolver, typename SocketType::resolver::iterator endpointIterator, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
	if (!errorCode) {
		typename SocketType::endpoint endpoint = *endpointIterator;
		
		std::lock_guard<std::mutex> l(socketMutex);

		if (socket) {
			socket->async_connect(endpoint,
								[this, resolver, nextEndpoint = ++endpointIterator, handlerLock](const boost::system::error_code& ec) {
									ConnectPostConnectHandler(resolver, nextEndpoint, ec, handlerLock);
								});
			return;
		}
	}
	
	if (errorCode && errorCode != boost::asio::error::operation_aborted) {
		std::lock_guard<std::mutex> l(socketMutex);

		if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_Error, this, SM_ErrorType_CONNECT_ERROR, errorCode.value()));
	}

	delete resolver;
	delete handlerLock;
}

template <class SocketType>
void Socket<SocketType>::ConnectPostConnectHandler(typename SocketType::resolver* resolver, typename SocketType::resolver::iterator endpointIterator, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
	if (!errorCode) {
		{ // lock
			std::lock_guard<std::mutex> l(socketMutex);

			if (socket) {
				callbackHandler.AddCallback(new Callback(CallbackEvent_Connect, this));
			}
		} // ~lock

		ReceiveHandler(new char[16384], 16384, 0, boost::system::errc::make_error_code(boost::system::errc::success), handlerLock);
		
		delete resolver;
			
		return;
	} else if (endpointIterator != typename SocketType::resolver::iterator()) {
		{ // lock
			std::lock_guard<std::mutex> l(socketMutex);

			if (socket) {
				socket->close();
			}
		} // ~lock

		ConnectPostResolveHandler(resolver, endpointIterator, boost::system::errc::make_error_code(boost::system::errc::success), handlerLock);
			
		return;
	}

	if (errorCode && errorCode != boost::asio::error::operation_aborted) {
		std::lock_guard<std::mutex> l(socketMutex);

		if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_Error, this, SM_ErrorType_CONNECT_ERROR, errorCode.value()));
	}

	delete resolver;
	delete handlerLock;
}

template <class SocketType>
bool Socket<SocketType>::Disconnect() {
	std::lock_guard<std::mutex> l(socketMutex);

	if (!socket) return false;

	try {
		socket->close();

		return true;
	} catch (std::exception&) {
	}

	return false;
}

template <class SocketType>
bool Socket<SocketType>::Listen() {
	return false;
}

template<> void Socket<tcp>::ListenIncomingHandler(tcp::socket* newAsioSocket, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock);

template<>
bool Socket<tcp>::Listen() {
	std::shared_lock<std::shared_mutex>* handlerLock = NULL;
	tcp::socket* nextAsioSocket = NULL;

	try {
		if (!localEndpoint) throw std::logic_error("local endpoint not initialized, call bind() first");

		if (!tcpAcceptor) {
			tcpAcceptorMutex = new std::mutex();

			std::lock_guard<std::mutex> tcpAcceptorLock(*tcpAcceptorMutex);
			std::lock_guard<std::mutex> locelEndpointLock(*localEndpointMutex);

			tcpAcceptor = new tcp::acceptor(*socketHandler.ioService, *localEndpoint);

			while (!socketOptionQueue.empty()) {
				SetOption(socketOptionQueue.front()->option, socketOptionQueue.front()->value, false);
				delete socketOptionQueue.front();
				socketOptionQueue.pop();
			}
		}
	
		std::lock_guard<std::mutex> l(*tcpAcceptorMutex);

		handlerLock = new std::shared_lock<std::shared_mutex>(handlerMutex);

		nextAsioSocket = new tcp::socket(*socketHandler.ioService);

		tcpAcceptor->async_accept(*nextAsioSocket,
								  [this, nextAsioSocket, handlerLock](const boost::system::error_code& ec) {
									  ListenIncomingHandler(nextAsioSocket, ec, handlerLock);
								  });

		return true;
	} catch (std::exception&) {
		if (handlerLock) delete handlerLock;
		if (nextAsioSocket) delete nextAsioSocket;
	}

	return false;
}

template <class SocketType>
void Socket<SocketType>::ListenIncomingHandler(tcp::socket* newAsioSocket, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
	// invalid
}
template<>
void Socket<tcp>::ListenIncomingHandler(tcp::socket* newAsioSocket, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
	if (!errorCode) {
		std::lock_guard<std::mutex> l(*tcpAcceptorMutex);

		if (tcpAcceptor) {
			Socket<tcp>* newSocket = socketHandler.CreateSocket<tcp>(sm_sockettype);
			newSocket->socket = newAsioSocket;
			callbackHandler.AddCallback(new Callback(CallbackEvent_Incoming, this, newSocket, newAsioSocket->remote_endpoint()));

			newSocket->ReceiveHandler(new char[16384], 16384, 0, boost::system::errc::make_error_code(boost::system::errc::success), new std::shared_lock<std::shared_mutex>(newSocket->handlerMutex));

			tcp::socket* nextAsioSocket = new tcp::socket(*socketHandler.ioService);

			tcpAcceptor->async_accept(*nextAsioSocket,
									  [this, nextAsioSocket, handlerLock](const boost::system::error_code& ec) {
										  ListenIncomingHandler(nextAsioSocket, ec, handlerLock);
									  });
			return;
		}
	}

	if (errorCode && errorCode != boost::asio::error::operation_aborted) {
		callbackHandler.AddCallback(new Callback(CallbackEvent_Error, this, SM_ErrorType_LISTEN_ERROR, errorCode.value()));
	}

	delete newAsioSocket;
	delete handlerLock;
}

template <class SocketType>
bool Socket<SocketType>::Send(const std::string& data, bool async) {
	char* buf = NULL;
	std::shared_lock<std::shared_mutex>* handlerLock = NULL;

	try {
		if (!socket && !tcpAcceptor) throw std::logic_error("can't send without connection");

		if (async) {
			char* buf = new char[data.length()];
			memcpy(buf, data.data(), data.length());

			sendQueueLength++;

			handlerLock = new std::shared_lock<std::shared_mutex>(handlerMutex);

			std::lock_guard<std::mutex> l(socketMutex);

			if (socket) {
				socket->async_send(boost::asio::buffer(buf, data.length()),
								   [this, buf, handlerLock](const boost::system::error_code& ec, size_t bytesTransferred) {
									   SendPostSendHandler(buf, bytesTransferred, ec, handlerLock);
								   });
			} else {
				throw new std::logic_error("Operation cancelled.");
			}
		} else {
			std::lock_guard<std::mutex> l(socketMutex);

			if (socket) {
				socket->send(boost::asio::buffer(data, data.length()));
			} else {
				throw std::logic_error("Operation cancelled.");
			}
		}

		return true;
	} catch (std::exception&) {
		if (buf) delete[] buf;
		if (handlerLock) delete handlerLock;
	}

	return false;
}

template <class SocketType>
void Socket<SocketType>::SendPostSendHandler(char* buf, size_t bytesTransferred, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
// TODO: handle incomplete sends
	if (--sendQueueLength == 0 && sendqueueEmptyCallback) {
		std::lock_guard<std::mutex> l(socketMutex);

		if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_SendQueueEmpty, this));
	}

	if (errorCode && errorCode != boost::asio::error::operation_aborted) {
		std::lock_guard<std::mutex> l(socketMutex);
		
		if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_Error, this, SM_ErrorType_SEND_ERROR, errorCode.value()));
	}

	delete[] buf;
	delete handlerLock;
}

template <class SocketType>
bool Socket<SocketType>::SendTo(const std::string& data, const char* hostname, uint16_t port, bool async) {
	return false;
}

template <>
bool Socket<udp>::SendTo(const std::string& data, const char* hostname, uint16_t port, bool async) {
	char* buf = NULL;
	udp::resolver* resolver = NULL;
	std::shared_lock<std::shared_mutex>* handlerLock = NULL;

	try {
		char sPort[6];
		snprintf(sPort, sizeof(sPort), "%hu", port);

		if (!socket) InitializeSocket();

		if (async) {
			buf = new char[data.length()];
			memcpy(buf, data.data(), data.length());

			sendQueueLength++;

			resolver = new udp::resolver(*socketHandler.ioService);
			handlerLock = new std::shared_lock<std::shared_mutex>(handlerMutex);

			resolver->async_resolve(udp::resolver::query(udp::v4(), hostname, sPort),
									[this, resolver, buf, bufLen = data.length(), handlerLock](const boost::system::error_code& ec, udp::resolver::iterator endpointIterator) {
										SendToPostResolveHandler(resolver, endpointIterator, buf, bufLen, ec, handlerLock);
									});
		} else {
			udp::resolver syncResolver(*socketHandler.ioService);

			udp::resolver::iterator endpointIterator = syncResolver.resolve(udp::resolver::query(udp::v4(), hostname, sPort));

			std::lock_guard<std::mutex> l(socketMutex);

			if (socket) {
				socket->send_to(boost::asio::buffer(data, data.length()), *endpointIterator);
			} else {
				throw std::logic_error("Operation cancelled.");
			}
		}

		return true;
	} catch (std::exception&) {
		if (resolver) delete resolver;
		if (buf) delete[] buf;
		if (handlerLock) delete handlerLock;
	}

	return false;
}

template <class SocketType>
void Socket<SocketType>::SendToPostResolveHandler(typename SocketType::resolver* resolver, typename SocketType::resolver::iterator endpointIterator, char* buf, size_t bufLen, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
	if (!errorCode) {
		typename SocketType::endpoint endpoint = *endpointIterator;

		std::lock_guard<std::mutex> l(socketMutex);

		if (socket) {
			socket->async_send_to(boost::asio::buffer(buf, bufLen),
								endpoint,
								[this, resolver, nextEndpoint = ++endpointIterator, buf, bufLen, handlerLock](const boost::system::error_code& ec, size_t bytesTransferred) {
									SendToPostSendHandler(resolver, nextEndpoint, buf, bufLen, bytesTransferred, ec, handlerLock);
								});
			return;
		}
	}



	if (errorCode && errorCode != boost::asio::error::operation_aborted) {
		std::lock_guard<std::mutex> l(socketMutex);
		
		if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_Error, this, SM_ErrorType_NO_HOST, errorCode.value()));
	}
	
	delete resolver;
	delete[] buf;
	delete handlerLock;
}

template <class SocketType>
void Socket<SocketType>::SendToPostSendHandler(typename SocketType::resolver* resolver, typename SocketType::resolver::iterator endpointIterator, char* buf, size_t bufLen, size_t bytesTransferred, const boost::system::error_code& errorCode, std::shared_lock<std::shared_mutex>* handlerLock) {
	if (!errorCode) {
		if (--sendQueueLength == 0 && sendqueueEmptyCallback) {
			std::lock_guard<std::mutex> l(socketMutex);
		
			if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_SendQueueEmpty, this));
		}

	} else if (endpointIterator != typename SocketType::resolver::iterator()) {
		SendToPostResolveHandler(resolver, endpointIterator, buf, bufLen, boost::system::errc::make_error_code(boost::system::errc::success), handlerLock);
		return;
		
	} else {
		if (errorCode != boost::asio::error::operation_aborted) {
			std::lock_guard<std::mutex> l(socketMutex);
		
			if (socket) callbackHandler.AddCallback(new Callback(CallbackEvent_Error, this, SM_ErrorType_SEND_ERROR, errorCode.value()));
		}
	}

	delete resolver;
	delete[] buf;
	delete handlerLock;
}

template <class SocketType>
bool Socket<SocketType>::SetOption(SM_SocketOption so, int value, bool lock) {
	std::lock_guard<std::mutex>* l = NULL;

	try {
		if (socket) {
			if (lock) l = new std::lock_guard<std::mutex>(socketMutex);
			if (!socket) return false;

			switch (so) {
				case SM_SO_SocketBroadcast:
					socket->set_option(boost::asio::socket_base::broadcast(value!=0));
					break;
				case SM_SO_SocketReuseAddr:
					socket->set_option(boost::asio::socket_base::reuse_address(value!=0));
					break;
				case SM_SO_SocketKeepAlive:
					socket->set_option(boost::asio::socket_base::keep_alive(value!=0));
					break;
				case SM_SO_SocketLinger:
					socket->set_option(boost::asio::socket_base::linger(value>0, value));
					break;
				case SM_SO_SocketOOBInline:
					// TODO: implement?
					if (l) delete l;
					return false;
				case SM_SO_SocketSendBuffer:
					socket->set_option(boost::asio::socket_base::send_buffer_size(value));
					break;
				case SM_SO_SocketReceiveBuffer:
					socket->set_option(boost::asio::socket_base::receive_buffer_size(value));
					break;
				case SM_SO_SocketDontRoute:
					socket->set_option(boost::asio::socket_base::do_not_route(value!=0));
					break;
				case SM_SO_SocketReceiveLowWatermark:
					socket->set_option(boost::asio::socket_base::receive_low_watermark(value));
					break;
				case SM_SO_SocketReceiveTimeout:
					// TODO: implement?
					if (l) delete l;
					return false;
				case SM_SO_SocketSendLowWatermark:
					socket->set_option(boost::asio::socket_base::send_low_watermark(value));
					break;
				case SM_SO_SocketSendTimeout:
					// TODO: implement?
					if (l) delete l;
					return false;
				default:
					if (l) delete l;
					return false;
			}
		} else if (tcpAcceptor) {
			if (lock) l = new std::lock_guard<std::mutex>(*tcpAcceptorMutex);
			if (!tcpAcceptor) return false;

			switch (so) {
				case SM_SO_SocketBroadcast:
					tcpAcceptor->set_option(boost::asio::socket_base::broadcast(value!=0));
					break;
				case SM_SO_SocketReuseAddr:
					tcpAcceptor->set_option(boost::asio::socket_base::reuse_address(value!=0));
					break;
				case SM_SO_SocketKeepAlive:
					tcpAcceptor->set_option(boost::asio::socket_base::keep_alive(value!=0));
					break;
				case SM_SO_SocketLinger:
					tcpAcceptor->set_option(boost::asio::socket_base::linger(value>0, value));
					break;
				case SM_SO_SocketOOBInline:
					// TODO: implement?
					if (l) delete l;
					return false;
				case SM_SO_SocketSendBuffer:
					tcpAcceptor->set_option(boost::asio::socket_base::send_buffer_size(value));
					break;
				case SM_SO_SocketReceiveBuffer:
					tcpAcceptor->set_option(boost::asio::socket_base::receive_buffer_size(value));
					break;
				case SM_SO_SocketDontRoute:
					tcpAcceptor->set_option(boost::asio::socket_base::do_not_route(value!=0));
					break;
				case SM_SO_SocketReceiveLowWatermark:
					tcpAcceptor->set_option(boost::asio::socket_base::receive_low_watermark(value));
					break;
				case SM_SO_SocketReceiveTimeout:
					// TODO: implement?
					if (l) delete l;
					return false;
				case SM_SO_SocketSendLowWatermark:
					tcpAcceptor->set_option(boost::asio::socket_base::send_low_watermark(value));
					break;
				case SM_SO_SocketSendTimeout:
					// TODO: implement?
					if (l) delete l;
					return false;
				default:
					if (l) delete l;
					return false;
			}
		} else {
			socketOptionQueue.push(new SocketOption(so, value));
		}

		if (l) delete l;
		return true;
	} catch (std::exception&) {
		if (l) delete l;
		return false;
	}
}

template <class SocketType>
void Socket<SocketType>::InitializeSocket() {
	assert(!socket);

	std::lock_guard<std::mutex> l(socketMutex);

	if (!socket) {
		if (localEndpointMutex) {
			std::lock_guard<std::mutex> l(*localEndpointMutex);

			if (localEndpoint) {
				socket = new typename SocketType::socket(*socketHandler.ioService, *localEndpoint);
			} else {
				socket = new typename SocketType::socket(*socketHandler.ioService, typename SocketType::endpoint(SocketType::v4(), 0));
			}
		} else {
			socket = new typename SocketType::socket(*socketHandler.ioService);
		}
		
		if (!socket->is_open()) socket->open(SocketType::v4());

		while (!socketOptionQueue.empty()) {
			SetOption(socketOptionQueue.front()->option, socketOptionQueue.front()->value, false);
			delete socketOptionQueue.front();
			socketOptionQueue.pop();
		}
	}
}

template Socket<tcp>::Socket(SM_SocketType, tcp::socket*);
template Socket<tcp>::~Socket();
template bool Socket<tcp>::IsOpen();
template bool Socket<tcp>::Bind(const char*, uint16_t, bool);
template bool Socket<tcp>::Connect(const char*, uint16_t, bool);
template bool Socket<tcp>::Disconnect();
template bool Socket<tcp>::Send(const std::string&, bool);
template bool Socket<tcp>::SendTo(const std::string&, const char*, uint16_t, bool);
template bool Socket<tcp>::SetOption(SM_SocketOption, int, bool);

template Socket<udp>::Socket(SM_SocketType, udp::socket*);
template Socket<udp>::~Socket();
template bool Socket<udp>::IsOpen();
template bool Socket<udp>::Bind(const char*, uint16_t, bool);
template bool Socket<udp>::Connect(const char*, uint16_t, bool);
template bool Socket<udp>::Disconnect();
template bool Socket<udp>::Listen();
template bool Socket<udp>::Send(const std::string&, bool);
template bool Socket<udp>::SetOption(SM_SocketOption, int, bool);

