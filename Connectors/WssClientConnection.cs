using System.Net.WebSockets;
using System.Text;
using Newtonsoft.Json.Linq;
using UA_DataProcessor.Interfaces;

namespace UA_DataProcessor.Connectors
{
	public class WssClientConnection : IMessageConnection
	{
		private readonly Uri _wssEndpoint;
		private readonly string _apiKey;
		private readonly int _reconnectIntervalMs;
		private readonly int _pingIntervalSeconds;
		private readonly int _connectTimeoutSeconds;

		private volatile bool _running;
		private volatile bool _intentionalClose;

		private readonly object _clientLock = new object();
		private ClientWebSocket _client;

		public string ClientId { get; }

		public WssClientConnection(
			Uri wssEndpoint,
			string apiKey,
			string clientId,
			int reconnectIntervalMs,
			int pingIntervalSeconds,
			int connectTimeoutSeconds)
		{
			_wssEndpoint = wssEndpoint;
			_apiKey = apiKey;
			ClientId = clientId;
			_reconnectIntervalMs = reconnectIntervalMs;
			_pingIntervalSeconds = pingIntervalSeconds;
			_connectTimeoutSeconds = connectTimeoutSeconds;
		}

		public async Task RunAsync(Func<IMessageConnection, string, CancellationToken, Task> onMessageAsync, CancellationToken cancellationToken)
		{
			_running = true;
			_intentionalClose = false;

			while (!cancellationToken.IsCancellationRequested)
			{
				CancellationTokenSource pingCts = null;
				Task pingTask = null;

				try
				{
					ClientWebSocket client = CreateWebSocket();
					SetCurrentClient(client);

					Uri connectUri = BuildConnectUri();
					Console.WriteLine("Connecting WSS client to " + connectUri);

					bool connected = await ConnectWithTimeoutAsync(client, connectUri, cancellationToken).ConfigureAwait(false);
					if (!connected)
					{
						Console.WriteLine("Initial WSS connection attempt timed out; will retry.");
						await DelayReconnectAsync(cancellationToken).ConfigureAwait(false);
						continue;
					}

					Console.WriteLine("WSS client connected.");
					await SendProtocolPingAsync(cancellationToken).ConfigureAwait(false);

					pingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
					pingTask = StartPingLoopAsync(pingCts.Token);

					await ReceiveLoopAsync(client, onMessageAsync, cancellationToken).ConfigureAwait(false);
				}
				catch (OperationCanceledException)
				{
					throw;
				}
				catch (Exception ex)
				{
					Console.WriteLine("WSS connection error: " + ex.Message);
				}
				finally
				{
					if (pingCts != null)
					{
						pingCts.Cancel();
						pingCts.Dispose();
					}

					if (pingTask != null)
					{
						try
						{
							await pingTask.ConfigureAwait(false);
						}
						catch (OperationCanceledException)
						{
						}
					}

					DisposeCurrentClient();
				}

				if (!_running || _intentionalClose)
				{
					return;
				}

				await DelayReconnectAsync(cancellationToken).ConfigureAwait(false);
			}
		}

		public void Close()
		{
			_intentionalClose = true;
			_running = false;
			DisposeCurrentClient();
		}

		public async Task SendTextAsync(string text, CancellationToken cancellationToken)
		{
			ClientWebSocket client = GetCurrentClient();
			if (client == null || client.State != WebSocketState.Open)
			{
				return;
			}

			byte[] bytes = Encoding.UTF8.GetBytes(text);
			await client.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, cancellationToken).ConfigureAwait(false);
		}

		private ClientWebSocket CreateWebSocket()
		{
			ClientWebSocket client = new ClientWebSocket();
			client.Options.SetRequestHeader("Authorization", "ApiKey " + _apiKey);
			client.Options.SetRequestHeader("X-Api-Key", _apiKey);
			return client;
		}

		private Uri BuildConnectUri()
		{
			string baseUrl = _wssEndpoint.ToString();
			string separator = baseUrl.Contains("?") ? "&" : "?";
			string uri = baseUrl
				+ separator
				+ "clientId=" + Uri.EscapeDataString(ClientId)
				+ "&apiKey=" + Uri.EscapeDataString(_apiKey);

			return new Uri(uri);
		}

		private async Task<bool> ConnectWithTimeoutAsync(ClientWebSocket client, Uri connectUri, CancellationToken cancellationToken)
		{
			using CancellationTokenSource timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
			timeoutCts.CancelAfter(TimeSpan.FromSeconds(_connectTimeoutSeconds));

			try
			{
				await client.ConnectAsync(connectUri, timeoutCts.Token).ConfigureAwait(false);
				return client.State == WebSocketState.Open;
			}
			catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
			{
				return false;
			}
		}

		private Task StartPingLoopAsync(CancellationToken cancellationToken)
		{
			return Task.Run(async () =>
			{
				while (!cancellationToken.IsCancellationRequested)
				{
					await Task.Delay(TimeSpan.FromSeconds(_pingIntervalSeconds), cancellationToken).ConfigureAwait(false);
					await SendProtocolPingAsync(cancellationToken).ConfigureAwait(false);
				}
			}, cancellationToken);
		}

		private async Task SendProtocolPingAsync(CancellationToken cancellationToken)
		{
			JObject payload = new JObject
			{
				["type"] = "ping",
				["clientId"] = ClientId,
				["timestamp"] = DateTimeOffset.UtcNow.ToString("o")
			};

			await SendTextAsync(payload.ToString(Newtonsoft.Json.Formatting.None), cancellationToken).ConfigureAwait(false);
		}

		private async Task ReceiveLoopAsync(
			ClientWebSocket client,
			Func<IMessageConnection, string, CancellationToken, Task> onMessageAsync,
			CancellationToken cancellationToken)
		{
			byte[] receiveBuffer = new byte[8192];
			ArraySegment<byte> buffer = new ArraySegment<byte>(receiveBuffer);

			while (client.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
			{
				using MemoryStream messageBuffer = new MemoryStream();
				WebSocketReceiveResult receiveResult;

				do
				{
					receiveResult = await client.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);

					if (receiveResult.MessageType == WebSocketMessageType.Close)
					{
						await client.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", cancellationToken).ConfigureAwait(false);
						return;
					}

					messageBuffer.Write(receiveBuffer, 0, receiveResult.Count);
				}
				while (!receiveResult.EndOfMessage);

				if (receiveResult.MessageType != WebSocketMessageType.Text)
				{
					continue;
				}

				string message = Encoding.UTF8.GetString(messageBuffer.ToArray());
				await onMessageAsync(this, message, cancellationToken).ConfigureAwait(false);
			}
		}

		private async Task DelayReconnectAsync(CancellationToken cancellationToken)
		{
			Console.WriteLine("Scheduling reconnect in " + _reconnectIntervalMs + "ms");
			await Task.Delay(_reconnectIntervalMs, cancellationToken).ConfigureAwait(false);
		}

		private void SetCurrentClient(ClientWebSocket client)
		{
			lock (_clientLock)
			{
				_client = client;
			}
		}

		private ClientWebSocket GetCurrentClient()
		{
			lock (_clientLock)
			{
				return _client;
			}
		}

		private void DisposeCurrentClient()
		{
			lock (_clientLock)
			{
				if (_client != null)
				{
					try
					{
						_client.Dispose();
					}
					catch (Exception ex)
					{
						Console.WriteLine("Failed to dispose current WSS client: " + ex.Message);
					}

					_client = null;
				}
			}
		}
	}
}

