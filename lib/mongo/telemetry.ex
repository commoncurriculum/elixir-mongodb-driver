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

  @type attempt :: %{database: String.t() | nil, address: String.t() | nil, error: Exception.t() | nil} | nil

  @doc """
  Emits `[:mongodb_driver, :command, :start | :stop | :exception]` around one logical operation:
  every retry and the server selection of each. `fun` returns the operation's result and the
  last attempt: the database and server it reached and the error it got.
  """
  @spec span(GenServer.server(), BSON.document(), Keyword.t(), (-> {result, attempt})) :: result when result: var
  def span(topology, cmd, opts, fun) do
    {command, collection, _params} = parts(cmd)
    %{database: database, servers: servers} = topology(topology)

    metadata = %{
      command: command,
      collection: collection,
      cmd: cmd,
      database: opts[:database] || database,
      servers: servers,
      options: Keyword.get(opts, :telemetry_options, [])
    }

    :telemetry.span([:mongodb_driver, :command], metadata, fn ->
      {result, attempt} = fun.()
      {result, Map.merge(metadata, stop(attempt, result))}
    end)
  end

  @doc false
  @spec register_topology(String.t() | nil, [String.t()]) :: :ok
  def register_topology(database, servers) do
    {:ok, _owner} = Registry.register(__MODULE__, self(), %{database: database, servers: servers})
    :ok
  end

  defp topology(topology) do
    with pid when is_pid(pid) <- GenServer.whereis(topology),
         [{^pid, info}] <- Registry.lookup(__MODULE__, pid) do
      info
    else
      _ -> %{database: nil, servers: []}
    end
  end

  defp stop(nil, {:error, error}), do: %{address: error_host(error), error: error}
  defp stop(nil, _result), do: %{address: nil, error: nil}

  defp stop(%{database: database} = attempt, _result) when is_binary(database),
    do: attempt

  defp stop(attempt, _result), do: Map.delete(attempt, :database)

  defp error_host(%Mongo.Error{host: host}) when is_binary(host), do: host
  defp error_host(_error), do: nil

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
