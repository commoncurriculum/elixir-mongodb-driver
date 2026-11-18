defmodule Mongo.Telemetry do
  @moduledoc false

  @collection_commands [
    :aggregate,
    :collMod,
    :collStats,
    :count,
    :create,
    :createIndexes,
    :delete,
    :distinct,
    :drop,
    :dropIndexes,
    :find,
    :findAndModify,
    :insert,
    :killCursors,
    :listIndexes,
    :mapReduce,
    :update,
    :validate
  ]

  @attempt {__MODULE__, :attempt}

  @doc """
  Emits `[:mongodb_driver, :command, :start | :stop | :exception]` around one logical operation:
  every retry and the server selection of each. The stop carries the database and server of the
  last reply the operation got (see `attempt/2`) and its error. `fun`'s result is returned as is.
  `:caller` is the process the operation runs for: the session's caller for commit and abort,
  which run in the session process, and the emitting process otherwise.
  """
  @spec span(GenServer.server() | nil, BSON.document(), Keyword.t(), (-> result)) :: result when result: var
  def span(topology, cmd, opts, fun) do
    {command, collection, _params} = parts(cmd)
    %{database: database, servers: servers} = topology(topology)

    metadata = %{
      command: command,
      collection: collection,
      cmd: cmd,
      database: opts[:database] || database,
      servers: servers,
      options: Keyword.get(opts, :telemetry_options, []),
      caller: Keyword.get(opts, :telemetry_caller, self())
    }

    outer = Process.delete(@attempt)

    try do
      :telemetry.span([:mongodb_driver, :command], metadata, fn ->
        result = fun.()
        {result, Map.merge(metadata, stop(Process.get(@attempt), result))}
      end)
    after
      if outer, do: Process.put(@attempt, outer), else: Process.delete(@attempt)
    end
  end

  @doc false
  # Records one attempt's reply, in the process that got it, for the span around it.
  @spec attempt(map, Exception.t() | nil) :: :ok
  def attempt(event, error) do
    Process.put(@attempt, %{database: Map.get(event, :database_name), address: Map.get(event, :address), error: error})
    :ok
  end

  @doc false
  @spec register_topology(String.t() | nil, [String.t()]) :: :ok
  def register_topology(database, servers) do
    _ = Registry.register(__MODULE__, self(), %{database: database, servers: servers})
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp topology(nil), do: %{database: nil, servers: []}

  defp topology(topology) do
    with pid when is_pid(pid) <- GenServer.whereis(topology),
         [{^pid, info}] <- Registry.lookup(__MODULE__, pid) do
      info
    else
      _ -> %{database: nil, servers: []}
    end
  end

  defp stop(attempt, result) do
    attempt = attempt || %{database: nil, address: nil, error: nil}

    {address, error} =
      case result do
        {:error, %Mongo.Error{host: host} = error} when is_binary(host) -> {host, error}
        {:error, error} -> {attempt.address, error}
        _result -> {attempt.address, attempt.error}
      end

    stop = %{address: address, error: error}

    case attempt.database do
      database when is_binary(database) -> Map.put(stop, :database, database)
      _none -> stop
    end
  end

  @spec parts(BSON.document()) :: {atom, String.t() | nil, Keyword.t()}
  def parts(cmd) do
    [{command, value} | _] = cmd
    {_, params} = Keyword.pop_first(cmd, command)

    case command do
      :getMore ->
        {collection, params} = Keyword.pop_first(params, :collection)
        {command, collection, params}

      :renameCollection when is_binary(value) ->
        {command, value |> String.split(".", parts: 2) |> List.last(), params}

      command when command in @collection_commands and is_binary(value) ->
        {command, value, params}

      command ->
        {command, nil, params}
    end
  end

  @spec reply_error(map) :: Mongo.WriteError.t() | Mongo.Error.t() | nil
  def reply_error(%{"writeErrors" => write_errors} = doc), do: %Mongo.WriteError{n: doc["n"], ok: doc["ok"], write_errors: write_errors}
  def reply_error(%{"writeConcernError" => error}), do: Mongo.Error.exception(error)
  def reply_error(_doc), do: nil
end
