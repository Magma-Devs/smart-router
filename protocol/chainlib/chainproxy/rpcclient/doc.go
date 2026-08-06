// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

/*
package rpcclient is a fork of go-ethereum's rpc package, reduced to the client
half: it dials a JSON-RPC 2.0 node over HTTP, WebSocket, or IPC and reads
responses and subscription notifications from it.

The server half (Server, ServeCodec/ServeHTTP/ServeListener, the WebSocket and
IPC listeners, and the subscription Notifier) was removed — nothing in
smart-router serves RPC from this package, it only consumes upstream nodes.
Method-registration machinery (serviceRegistry, the handler, the codecs) is
kept because the client uses it to dispatch server-to-client notifications.

# RPC Methods

Methods that satisfy the following criteria are made available for remote access:

  - method must be exported
  - method returns 0, 1 (response or error) or 2 (response and error) values

An example method:

	func (s *CalcService) Add(a, b int) (int, error)

When the returned error isn't nil the returned integer is ignored and the error is sent
back to the client. Otherwise the returned integer is sent back to the client.

Optional arguments are supported by accepting pointer values as arguments. E.g. if we want
to do the addition in an optional finite field we can accept a mod argument as pointer
value.

	func (s *CalcService) Add(a, b int, mod *int) (int, error)

This RPC method can be called with 2 integers and a null value as third argument. In that
case the mod argument will be nil. Or it can be called with 3 integers, in that case mod
will be pointing to the given third argument. Since the optional argument is the last
argument the RPC package will also accept 2 integers as arguments. It will pass the mod
argument as nil to the RPC method.

# Subscriptions

The package also supports the publish subscribe pattern through the use of subscriptions.
A method that is considered eligible for notifications must satisfy the following
criteria:

  - method must be exported
  - first method argument type must be context.Context
  - method must have return types (rpc.Subscription, error)

An example method:

	func (s *BlockChainService) NewBlocks(ctx context.Context) (rpc.Subscription, error) {
		...
	}

On the client side, Client.Subscribe issues such a call over a streaming
transport (WebSocket or IPC) and returns a *ClientSubscription that delivers
notifications on the supplied channel.

Subscriptions are deleted when the client sends an unsubscribe request or when the
connection which was used to create the subscription is closed.

For more information about subscriptions, see https://github.com/ethereum/go-ethereum/wiki/RPC-PUB-SUB.
*/
package rpcclient
