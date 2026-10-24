defmodule Mongo.Stream do
  @moduledoc """
    This module provides a stream implementation that automatically checks out a session when the stream is started and an Enumerable
    protocol that checks it back in when the stream has been consumed.

    The first batch is decoded when the stream is created. Later (`getMore`) batches are decoded a few documents
    at a time while the stream is consumed, and the next batch is requested in the background while the current
    one is consumed. That request runs in another process; pass `prefetch: false` to run it in the caller.

    The background request is tied to the process that consumes the stream. A stream suspended in one process
    (for example with `Enumerable.reduce/3` and `:suspend`) can be resumed in another one, but not after the
    first process has exited. A batch fetched in the background is also dropped if the stream does not get
    to it within 10 minutes, the server's default cursor timeout (`cursorTimeoutMillis`). Cursors created
    with `no_cursor_timeout: true` are never prefetched; with a longer server timeout, use `prefetch: false`.
  """

  alias Mongo.Session
  alias Mongo.Error

  defstruct [
    :topology_pid,
    :session,
    :cursor,
    :coll,
    :docs,
    :cmd,
    :opts,
    # Decided when the stream is created: whether getMores may be requested in the background.
    :prefetch_enabled,
    # The state of the next getMore: nil, `{:from, bytes}` (request it once at most that many
    # bytes of the current batch are left) or the pid of a running Mongo.Stream.Prefetch.
    :prefetch
  ]

  def new(topology_pid, cmd, opts) do
    ## check, if retryable reads are enabled
    opts = Mongo.retryable_reads(opts)

    with {type, session} <- checkout_session(topology_pid, opts) do
      # The first batch is decoded eagerly, so `docs` is a plain list as before.
      case Mongo.exec_command_session(session, cmd, opts) do
        {:ok, %{"ok" => ok, "cursor" => cursor}} when ok == 1 ->
          %Mongo.Stream{
            topology_pid: topology_pid,
            session: {type, session},
            cursor: cursor["id"],
            coll: cursor["ns"],
            docs: cursor["firstBatch"],
            cmd: cmd,
            opts: Keyword.put(opts, :session, session),
            prefetch_enabled: prefetch_enabled?(type, cmd, opts)
          }

        {:error, error} ->
          checkin_session(type, session, topology_pid)

          case Error.should_retry_read(error, cmd, opts) do
            true -> new(topology_pid, cmd, Keyword.put(opts, :read_counter, 2))
            false -> {:error, error}
          end
      end
    end
  end

  # Only for streams
  # - that own their session: a borrowed one may be in use by the caller, for example in a
  #   transaction;
  # - whose getMore returns at once: a tailable getMore waits for new data (Mongo.aggregate
  #   passes `prefetch: false` for change streams);
  # - whose cursor does not stay open for an unlimited time (see the moduledoc);
  # - not started with `prefetch: false`.
  defp prefetch_enabled?(type, cmd, opts) do
    type == :own and opts[:prefetch] != false and cmd[:tailable] != true and cmd[:noCursorTimeout] != true
  end

  defp checkout_session(topology_pid, opts) do
    case Mongo.get_session(opts) do
      nil ->
        with {:ok, session} <- Session.start_session(topology_pid, :read, opts) do
          {:own, session}
        end

      session ->
        {:borrowed, session}
    end
  end

  def checkin_session(:own, session, topology_pid) do
    Session.end_session(topology_pid, session)
  end

  def checkin_session(:borrowed, _session, _topology_pid) do
  end

  defimpl Enumerable do
    alias Mongo.Stream.Prefetch

    # Documents decoded at a time from a getMore batch.
    @chunk_size 100

    # Prefetch once a quarter of the batch, but at most 256 KB of it, has been consumed, so
    # that a consumer that stops early (Enum.take/2) rarely pays for a batch it never reads.
    @prefetch_after 256 * 1024

    def reduce(%Mongo.Stream{} = stream, acc, reduce_fun) do
      Stream.resource(fn -> stream end, &next/1, &close/1).(acc, reduce_fun)
    end

    def slice(_cursor), do: {:error, __MODULE__}
    def count(_stream), do: {:error, __MODULE__}
    def member?(_stream, _term), do: {:error, __MODULE__}

    defp next(%Mongo.Stream{docs: [], cursor: 0} = stream), do: {:halt, stream}

    # If handle_more/2 raises, close/1 gets this stream and asks for the reply again; it gets
    # {:exit, _} and closes the cursor it knows.
    defp next(%Mongo.Stream{docs: [], prefetch: pid} = stream) when is_pid(pid), do: handle_more(await_more(pid), %{stream | prefetch: nil})
    defp next(%Mongo.Stream{docs: []} = stream), do: handle_more(request_more(more_request(stream)), stream)

    defp next(%Mongo.Stream{docs: %BSON.LazyDocuments{} = lazy} = stream) do
      # Decode first: if decoding raises, no getMore is left running.
      {docs, rest} = BSON.LazyDocuments.take(lazy, @chunk_size)
      {docs, maybe_prefetch(%{stream | docs: rest})}
    end

    defp next(%Mongo.Stream{docs: docs} = stream), do: {docs, %{stream | docs: []}}

    defp close(%Mongo.Stream{prefetch: pid} = stream) when is_pid(pid) do
      cursor =
        case Prefetch.await(pid) do
          {:ok, {:ok, %{"cursor" => %{"id" => cursor_id}}}} -> cursor_id
          _no_reply -> stream.cursor
        end

      close(%{stream | prefetch: nil, cursor: cursor})
    end

    defp close(%Mongo.Stream{topology_pid: topology_pid, session: {type, session}, cursor: 0}) do
      Mongo.Stream.checkin_session(type, session, topology_pid)
    end

    defp close(%Mongo.Stream{topology_pid: topology_pid, session: {type, session}} = stream) do
      # The cursor may already be gone, for example after a failed getMore; the session is
      # checked in anyway.
      try do
        kill_cursors(stream)
      after
        Mongo.Stream.checkin_session(type, session, topology_pid)
      end
    end

    defp handle_more({:ok, %{"cursor" => %{"id" => cursor_id, "nextBatch" => batch}, "ok" => ok}}, stream) when ok == 1 do
      next_batch(batch, %{stream | cursor: cursor_id})
    end

    defp handle_more({:error, error}, _stream), do: raise(error)
    defp handle_more(other, _stream), do: raise(Error.exception("Unexpected getMore reply: #{inspect(other)}"))

    defp next_batch([], stream), do: {cmd_tailable?(stream), stream}
    # Decoded in chunks by next/1.
    defp next_batch(%BSON.LazyDocuments{} = docs, stream), do: {[], %{stream | docs: docs, prefetch: prefetch_from(stream, docs)}}
    defp next_batch(docs, stream), do: {docs, stream}

    # While the consumer works through a getMore batch, the next one is already requested,
    # so the round trip to the server overlaps with decoding and processing. Never during
    # the first batch.
    defp prefetch_from(%Mongo.Stream{prefetch_enabled: true, cursor: cursor}, %BSON.LazyDocuments{binary: binary}) when cursor != 0 do
      {:from, byte_size(binary) - min(@prefetch_after, div(byte_size(binary), 4))}
    end

    defp prefetch_from(_stream, _docs), do: nil

    defp maybe_prefetch(%Mongo.Stream{prefetch: {:from, remaining}, docs: rest} = stream) do
      case rest == [] or byte_size(rest.binary) <= remaining do
        true ->
          # Only the getMore arguments are copied to the new process, not the stream.
          request = more_request(stream)
          %{stream | prefetch: Prefetch.start(fn -> request_more(request) end)}

        false ->
          stream
      end
    end

    defp maybe_prefetch(stream), do: stream

    defp await_more(pid) do
      case Prefetch.await(pid) do
        {:ok, result} ->
          result

        {:raised, kind, reason, stacktrace} ->
          :erlang.raise(kind, reason, stacktrace)

        {:exit, reason} ->
          message =
            "the next batch, fetched in the background, is gone (#{Exception.format_exit(reason)}): " <>
              "the process that started the stream exited, or the stream did not get to the batch within 10 minutes. " <>
              "Pass `prefetch: false` for such streams"

          {:error, Error.exception(message)}
      end
    end

    defp more_request(%Mongo.Stream{session: {_type, session}} = stream) do
      cmd =
        [
          getMore: %BSON.LongNumber{value: stream.cursor},
          collection: only_coll(stream.coll),
          batchSize: stream.opts[:batch_size],
          maxTimeMS: stream.opts[:max_time]
        ]
        |> filter_nils()

      {session, cmd, Keyword.put(stream.opts, :lazy_cursor, true)}
    end

    defp request_more({session, cmd, opts}), do: Mongo.exec_command_session(session, cmd, opts)

    defp cmd_tailable?(%{cmd: cmd}) do
      case cmd[:tailable] == true do
        true -> []
        false -> :halt
      end
    end

    defp kill_cursors(%{session: {_type, session}} = stream) do
      cmd = [
        killCursors: only_coll(stream.coll),
        cursors: [%BSON.LongNumber{value: stream.cursor}]
      ]

      with {:ok, %{"cursorsAlive" => [], "cursorsNotFound" => [], "cursorsUnknown" => [], "ok" => ok}} when ok == 1 <- Mongo.exec_command_session(session, cmd, stream.opts) do
        :ok
      end
    end

    defp only_coll(coll) do
      [_db, coll] = String.split(coll, ".", parts: 2)
      coll
    end

    defp filter_nils(keyword) when is_list(keyword) do
      Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
    end
  end
end
