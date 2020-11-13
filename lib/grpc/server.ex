defmodule GRPC.Server do
  @moduledoc """
  A gRPC server which handles requests by calling user-defined functions.

  You should pass a `GRPC.Service` in when *use* the module:

      defmodule Greeter.Service do
        use GRPC.Service, name: "ping"

        rpc :SayHello, Request, Reply
        rpc :SayGoodbye, stream(Request), stream(Reply)
      end

      defmodule Greeter.Server do
        use GRPC.Server, service: Greeter.Service

        def say_hello(request, _stream) do
          Reply.new(message: "Hello")
        end

        def say_goodbye(request_enum, stream) do
          requests = Enum.map request_enum, &(&1)
          GRPC.Server.send_reply(stream, reply1)
          GRPC.Server.send_reply(stream, reply2)
        end
      end

  Your functions should accept a client request and a `GRPC.Server.Stream`.
  The request will be a `Enumerable.t`(created by Elixir's `Stream`) of requests
  if it's streaming. If a reply is streaming, you need to call `send_reply/2` to send
  replies one by one instead of returning reply in the end.
  """

  require Logger

  alias GRPC.Server.Stream

  defmacro __using__(opts) do
    quote bind_quoted: [service_mod: opts[:service]] do
      service_name = service_mod.__meta__(:name)

      Enum.each(service_mod.__rpc_calls__, fn {name, _, _} = rpc ->
        func_name = name |> to_string |> Macro.underscore()
        path = "/#{service_name}/#{name}"

        def __call_rpc__(unquote(path), stream) do
          GRPC.Server.call(
            unquote(service_mod),
            stream,
            unquote(Macro.escape(rpc)),
            unquote(String.to_atom(func_name))
          )
        end
      end)

      def __call_rpc__(_, stream) do
        raise GRPC.RPCError, status: :unimplemented
      end

      def __meta__(:service), do: unquote(service_mod)
    end
  end

  @type servers_map :: %{String.t() => [module]}
  @type servers_list :: module | [module]

  @doc false
  @spec call(atom, Stream.t(), tuple, atom) :: {:ok, Stream.t(), struct} | {:ok, struct}
  def call(
        service_mod,
        stream,
        {_, {req_mod, req_stream}, {res_mod, res_stream}} = _rpc,
        func_name
      ) do
    marshal_func = fn res -> service_mod.marshal(res_mod, res) end
    unmarshal_func = fn req -> service_mod.unmarshal(req_mod, req) end
    stream = %{stream | marshal: marshal_func, unmarshal: unmarshal_func}

    handle_request(req_stream, res_stream, stream, func_name)
  end

  defp handle_request(req_s, res_s, %{server: server} = stream, func_name) do
    if function_exported?(server, func_name, 2) do
      do_handle_request(req_s, res_s, stream, func_name)
    else
      raise GRPC.RPCError, status: :unimplemented
    end
  end

  defp do_handle_request(
         false = req_stream,
         res_stream,
         %{unmarshal: unmarshal, adapter: adapter} = stream,
         func_name
       ) do
    {:ok, data, stream} = adapter.read_body(stream)
    message = GRPC.Message.from_data(data)
    request = unmarshal.(message)
    do_handle_request(req_stream, res_stream, stream, func_name, request)
  end

  defp do_handle_request(
         true = req_stream,
         res_stream,
         %{unmarshal: unmarshal, adapter: adapter} = stream,
         func_name
       ) do
    reading_stream =
      adapter.reading_stream(stream)
      |> Elixir.Stream.map(&unmarshal.(&1))

    do_handle_request(req_stream, res_stream, stream, func_name, reading_stream)
  end

  defp do_handle_request(false, false, %{server: server_mod} = stream, func_name, request) do
    reply = apply(server_mod, func_name, [request, stream])
    {:ok, stream, reply}
  end

  defp do_handle_request(false, true, %{server: server_mod} = stream, func_name, request) do
    apply(server_mod, func_name, [request, stream])
    {:ok, stream}
  end

  defp do_handle_request(true, false, %{server: server_mod} = stream, func_name, req_stream) do
    reply = apply(server_mod, func_name, [req_stream, stream])
    {:ok, stream, reply}
  end

  defp do_handle_request(true, true, %{server: server_mod} = stream, func_name, req_stream) do
    apply(server_mod, func_name, [req_stream, stream])
    {:ok, stream}
  end

  # Start the gRPC server. Only used in starting a server manually using `GRPC.Server.start(servers)`
  #
  # A generated `port` will be returned if the port is `0`.
  #
  # ## Examples
  #
  #     iex> {:ok, _, port} = GRPC.Server.start(Greeter.Server, 50051)
  #     iex> {:ok, _, port} = GRPC.Server.start(Greeter.Server, 0, ip: {:local, "path/to/unix.sock"})
  #
  # ## Options
  #
  #   * `:cred` - a credential created by functions of `GRPC.Credential`,
  #               an insecure server will be created without this option
  #   * `:adapter` - use a custom server adapter instead of default `GRPC.Adapter.Cowboy`
  @doc false
  @spec start(servers_list, non_neg_integer, Keyword.t()) :: {atom, any, non_neg_integer}
  def start(servers, port, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, GRPC.Adapter.Cowboy)
    servers = GRPC.Server.servers_to_map(servers)
    adapter.start(servers, port, opts)
  end

  # Stop the server
  #
  # ## Examples
  #
  #     iex> GRPC.Server.stop(Greeter.Server)
  #
  # ## Options
  #
  #   * `:adapter` - use a custom adapter instead of default `GRPC.Adapter.Cowboy`
  @doc false
  @spec stop(servers_list, Keyword.t()) :: any
  def stop(servers, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, GRPC.Adapter.Cowboy)
    servers = GRPC.Server.servers_to_map(servers)
    adapter.stop(servers)
  end

  @doc """
  DEPRECATED. Use `send_reply/2` instead
  """
  @deprecated "Use send_reply/2 instead"
  def stream_send(stream, reply) do
    send_reply(stream, reply)
  end

  @doc """
  Send streaming reply.

  ## Examples

      iex> GRPC.Server.send_reply(stream, reply)
  """
  @spec send_reply(Stream.t(), struct) :: Stream.t()
  def send_reply(%{adapter: adapter, marshal: marshal} = stream, reply) do
    stream =
      if adapter.has_sent_headers?(stream) do
        stream
      else
        send_headers(stream, %{})
      end

    {:ok, data, _size} = reply |> marshal.() |> GRPC.Message.to_data(%{iolist: true})
    adapter.send_reply(stream, data)
    stream
  end

  @doc """
  Send custom metadata(headers).

  You can send headers only once, before that you can set headers using `set_headers/2`.
  """
  @spec send_headers(Stream.t(), map) :: Stream.t()
  def send_headers(%{adapter: adapter} = stream, headers) do
    adapter.send_headers(stream, headers)
  end

  @doc """
  Set custom metadata(headers).

  You can set headers more than once.
  """
  @spec set_headers(Stream.t(), map) :: Stream.t()
  def set_headers(%{adapter: adapter} = stream, headers) do
    adapter.set_headers(stream, headers)
  end

  @doc """
  Set custom trailers, which will be sent in the end.
  """
  @spec set_trailers(Stream.t(), map) :: Stream.t()
  def set_trailers(%{adapter: adapter} = stream, trailers) do
    adapter.set_resp_trailers(stream, trailers)
  end

  @doc false
  def send_trailers(%{adapter: adapter} = stream, trailers) do
    adapter.send_trailers(stream, trailers)
  end

  @doc false
  @spec service_name(String.t()) :: String.t()
  def service_name(path) do
    ["", name | _] = String.split(path, "/")
    name
  end

  @doc false
  @spec servers_to_map(servers_list) :: servers_map
  def servers_to_map(servers) do
    Enum.reduce(List.wrap(servers), %{}, fn s, acc ->
      Map.put(acc, s.__meta__(:service).__meta__(:name), s)
    end)
  end
end
