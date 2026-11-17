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

  @type response :: {BSON.document(), map, integer, non_neg_integer} | nil

  @spec span(BSON.document(), Keyword.t(), (-> {result, response})) :: result when result: var
  def span(cmd, opts, fun) do
    case Keyword.has_key?(cmd, :isMaster) do
      true ->
        {result, _response} = fun.()
        result

      false ->
        {command, collection, params} = parts(cmd)

        metadata = %{
          command: command,
          collection: collection,
          params: params,
          database: opts[:database],
          caller: Keyword.get(opts, :caller, self()),
          options: Keyword.get(opts, :telemetry_options, [])
        }

        :telemetry.span([:mongodb_driver, :command], metadata, fn ->
          {result, response} = fun.()
          {result, metadata |> Map.merge(server(response, result)) |> Map.put(:error, error(result))}
        end)
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

  defp server({_doc, event, _flags, _duration}, _result), do: %{database: event.database_name, address: Map.get(event, :address)}
  defp server(nil, {:error, %Mongo.Error{host: host}}) when is_binary(host), do: %{address: host}
  defp server(nil, _result), do: %{address: nil}

  defp error({:ok, {_flags, doc}}), do: reply_error(doc)
  defp error({:error, error}), do: error
end
