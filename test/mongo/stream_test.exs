defmodule Mongo.StreamTest do
  use CollectionCase

  setup %{pid: top, test: test} do
    # A collection per test: the cursor tests count the commands sent on it.
    coll = "stream_#{:erlang.phash2(test)}"
    {:ok, _} = Mongo.delete_many(top, coll, %{})
    {:ok, _} = Mongo.insert_many(top, coll, Enum.map(1..1_000, fn i -> %{index: i, name: "doc #{i}", tags: ["a", "b"]} end))
    [coll: coll]
  end

  test "find returns every document in order for small and large batches", %{pid: top, coll: coll} do
    for batch_size <- [1, 7, 100, 1_000, 5_000] do
      indexes =
        top
        |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: batch_size)
        |> Enum.map(& &1["index"])

      assert indexes == Enum.to_list(1..1_000)
    end
  end

  test "aggregate returns decoded documents", %{pid: top, coll: coll} do
    assert [%{"index" => 1, "name" => "doc 1", "tags" => ["a", "b"]}] =
             top
             |> Mongo.aggregate(coll, [%{"$sort" => %{index: 1}}, %{"$limit" => 1}, %{"$project" => %{_id: 0}}])
             |> Enum.to_list()
  end

  test "halting a stream early still closes the cursor", %{pid: top, coll: coll, catcher: catcher} do
    assert [1, 2, 3] == top |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: 100) |> Stream.map(& &1["index"]) |> Enum.take(3)

    assert catcher |> EventCatcher.succeeded_events() |> Enum.any?(&(&1.command_name == :killCursors))
  end

  test "the docs of a stream are still a list of decoded documents", %{pid: top, coll: coll} do
    %Mongo.Stream{docs: docs} = stream = Mongo.find(top, coll, %{}, sort: %{index: 1}, batch_size: 10)
    assert is_list(docs)
    assert Enum.map(docs, & &1["index"]) == Enum.to_list(1..10)

    # Consume the stream so that its session and cursor are released.
    Stream.run(stream)
  end

  test "command event subscribers still get decoded batches", %{pid: top, coll: coll, catcher: catcher} do
    assert 1_000 == top |> Mongo.find(coll, %{}, batch_size: 400) |> Enum.count()

    batches =
      catcher
      |> EventCatcher.succeeded_events()
      |> Enum.filter(&(&1.command_name in [:find, :getMore]))
      |> Enum.map(fn event -> event.reply["cursor"]["firstBatch"] || event.reply["cursor"]["nextBatch"] end)

    assert batches |> Enum.map(&length/1) |> Enum.sort() == [200, 400, 400]
    assert %{"index" => _} = hd(hd(batches))
  end

  test "consuming a stream inside a transaction does not prefetch on the shared session", %{pid: top, coll: coll} do
    {result, background} =
      background_get_mores(fn ->
        Mongo.transaction(top, fn ->
          {:ok, top |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: 50) |> Enum.map(& &1["index"])}
        end)
      end)

    assert result == {:ok, Enum.to_list(1..1_000)}
    assert background == 0
  end

  test "the next getMore is prefetched, unless prefetch: false", %{pid: top, coll: coll} do
    assert {1_000, background} = background_get_mores(fn -> top |> Mongo.find(coll, %{}, batch_size: 100) |> Enum.count() end)
    # Every getMore but the first, which follows the eager first batch. The server may need
    # one more, empty getMore to notice the cursor is exhausted.
    assert background in 8..9

    assert {1_000, 0} = background_get_mores(fn -> top |> Mongo.find(coll, %{}, batch_size: 100, prefetch: false) |> Enum.count() end)
    assert_mailbox_empty()
  end

  test "halting while a getMore is prefetched closes the cursor", %{pid: top, coll: coll, catcher: catcher} do
    assert Enum.to_list(1..150) == top |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: 100) |> Stream.map(& &1["index"]) |> Enum.take(150)

    assert [:find, :getMore, :getMore, :killCursors] ==
             catcher |> EventCatcher.succeeded_events() |> Enum.map(& &1.command_name) |> Enum.filter(&(&1 in [:find, :getMore, :killCursors])) |> Enum.reverse()

    assert_mailbox_empty()
  end

  test "stopping early in a large batch does not fetch the next one", %{pid: top, coll: coll, catcher: catcher} do
    # The first chunk of the second batch is a fifth of it, before the prefetch starts.
    assert 550 == top |> Mongo.find(coll, %{}, batch_size: 500) |> Enum.take(550) |> length()

    assert [:find, :getMore, :killCursors] ==
             catcher |> EventCatcher.succeeded_events() |> Enum.map(& &1.command_name) |> Enum.filter(&(&1 in [:find, :getMore, :killCursors])) |> Enum.reverse()
  end

  test "tailable cursors do not prefetch", %{pid: top, coll: coll} do
    coll = coll <> "_capped"
    Mongo.drop_collection(top, coll)
    :ok = Mongo.create(top, coll, capped: true, size: 1_000_000)
    {:ok, _} = Mongo.insert_many(top, coll, Enum.map(1..300, &%{index: &1}))

    assert {250, 0} = background_get_mores(fn -> top |> Mongo.find(coll, %{}, tailable: true, batch_size: 100) |> Enum.take(250) |> length() end)
  end

  defmodule MatchStage do
    # A pipeline stage built by a query builder.
    defstruct [:filter]

    defimpl Mongo.Encoder do
      def encode(%{filter: filter}), do: %{"$match" => filter}
    end
  end

  test "aggregate pipelines whose stages are structs are prefetched", %{pid: top, coll: coll} do
    assert {1_000, background} = background_get_mores(fn -> top |> Mongo.aggregate(coll, [%MatchStage{filter: %{}}], batch_size: 100) |> Enum.count() end)
    assert background in 8..9
  end

  test "cursors without a timeout are not prefetched", %{pid: top, coll: coll} do
    assert {1_000, 0} = background_get_mores(fn -> top |> Mongo.find(coll, %{}, batch_size: 100, no_cursor_timeout: true) |> Enum.count() end)
  end

  test "change stream pipelines are not prefetched", %{pid: top, coll: coll} do
    for pipeline <- [[%{"$changeStream" => %{}}], [%{"$changeStream": %{}}], [["$changeStream": %{}]]] do
      # Creating the stream does not wait for changes; consuming it would.
      assert %Mongo.Stream{prefetch_enabled: false, session: {:own, session}, cursor: cursor_id} = Mongo.aggregate(top, coll, pipeline)
      {:ok, _} = Mongo.command(top, killCursors: coll, cursors: [%BSON.LongNumber{value: cursor_id}])
      Mongo.Stream.checkin_session(:own, session, top)
    end

    assert %Mongo.Stream{prefetch_enabled: true} = stream = Mongo.aggregate(top, coll, [%{"$match" => %{}}])
    Stream.run(stream)
  end

  defmodule ParkedStream do
    # Like GenStage.from_enumerable/2: keeps a suspended stream between demands and
    # handles unknown messages in between.
    use GenServer

    def init(stream) do
      {:ok,
       &Enumerable.reduce(stream, &1, fn
         doc, {docs, 1} -> {:suspend, {[doc | docs], 0}}
         doc, {docs, n} -> {:cont, {[doc | docs], n - 1}}
       end)}
    end

    def handle_call({:demand, n}, _from, continuation) do
      case continuation.({:cont, {[], n}}) do
        {:suspended, {docs, 0}, continuation} -> {:reply, {:more, length(docs)}, continuation}
        {_done, {docs, _n}} -> {:reply, {:done, length(docs)}, :done}
      end
    end

    def handle_info(_message, state), do: {:noreply, state}
  end

  test "a stream suspended between demands still gets its prefetched batches", %{pid: top, coll: coll} do
    {:ok, pid} = GenServer.start_link(ParkedStream, Mongo.find(top, coll, %{}, batch_size: 100))

    count = fn count, total ->
      case GenServer.call(pid, {:demand, 50}, 30_000) do
        {:more, n} ->
          # Leave the server idle while a prefetched getMore completes.
          Process.sleep(5)
          count.(count, total + n)

        {:done, n} ->
          total + n
      end
    end

    assert 1_000 == count.(count, 0)
  end

  test "a caller that traps exits gets no messages from the prefetch", %{pid: top, coll: coll} do
    Process.flag(:trap_exit, true)
    assert 1_000 == top |> Mongo.find(coll, %{}, batch_size: 100) |> Enum.count()
    assert_mailbox_empty()
  end

  test "a failed prefetched getMore raises the Mongo.Error", %{pid: top, coll: coll} do
    %Mongo.Stream{cursor: cursor_id} = stream = Mongo.find(top, coll, %{}, sort: %{index: 1}, batch_size: 100)

    kill = fn
      %{"index" => 101} -> {:ok, _} = Mongo.command(top, killCursors: coll, cursors: [%BSON.LongNumber{value: cursor_id}])
      _doc -> :ok
    end

    assert_raise Mongo.Error, fn -> stream |> Stream.each(kill) |> Stream.run() end
    assert_mailbox_empty()
  end

  defmodule FailingDecoder do
    def document(_binary), do: raise(ArgumentError, "bad document")
  end

  test "a batch that fails to decode raises without leaving a getMore running", %{pid: top, coll: coll, catcher: catcher} do
    stream = Mongo.find(top, coll, %{}, batch_size: 10)
    # A batch whose decoding would start a prefetch once its first chunk is taken.
    stream = %{stream | docs: %BSON.LazyDocuments{binary: <<0x03, "0", 0, 5, 0, 0, 0, 0>>, decoder: FailingDecoder}, prefetch: {:from, 1_000_000}}

    assert {_raised, 0} = background_get_mores(fn -> assert_raise(Mongo.Error, fn -> Enum.to_list(stream) end) end)
    assert catcher |> EventCatcher.succeeded_events() |> Enum.any?(&(&1.command_name == :killCursors))
    assert_mailbox_empty()
  end

  test "a stream resumed after the process that suspended it exited raises a clear error", %{pid: top, coll: coll} do
    stream = Mongo.find(top, coll, %{}, batch_size: 100)
    test = self()

    # Suspend inside a process that then exits, after the prefetch of the third batch started.
    spawn(fn ->
      {:suspended, _acc, continuation} = Enumerable.reduce(stream, {:cont, 0}, fn _doc, n -> if n == 150, do: {:suspend, n}, else: {:cont, n + 1} end)
      send(test, {:continuation, continuation})
    end)

    assert_receive {:continuation, continuation}
    error = assert_raise Mongo.Error, fn -> continuation.({:cont, 0}) end
    assert error.message =~ "prefetch: false"
  end

  test "a prefetched getMore that raises re-raises in the stream", %{pid: top, coll: coll} do
    stream = Mongo.find(top, coll, %{}, batch_size: 100)
    pid = Mongo.Stream.Prefetch.start(fn -> raise ArgumentError, "boom" end)

    assert_raise ArgumentError, "boom", fn -> Enum.to_list(%{stream | prefetch: pid}) end
    assert_mailbox_empty()
  end

  test "a failed getMore raises the Mongo.Error", %{pid: top, coll: coll} do
    %Mongo.Stream{cursor: cursor_id} = stream = Mongo.find(top, coll, %{}, batch_size: 10)
    {:ok, _} = Mongo.command(top, killCursors: coll, cursors: [%BSON.LongNumber{value: cursor_id}])

    assert_raise Mongo.Error, fn -> Enum.to_list(stream) end
  end

  defp assert_mailbox_empty() do
    assert {:messages, []} == Process.info(self(), :messages)
  end

  # Runs fun and counts the getMore commands that processes other than the caller sent.
  defp background_get_mores(fun) do
    test = self()
    mfa = {Mongo, :exec_command_session, 3}

    tracer =
      spawn(fn ->
        receive_calls = fn receive_calls, count ->
          receive do
            {:trace, pid, :call, {Mongo, :exec_command_session, [_session, [{:getMore, _cursor} | _cmd], _opts]}} when pid != test ->
              receive_calls.(receive_calls, count + 1)

            {:trace, _pid, _event, _call} ->
              receive_calls.(receive_calls, count)

            {:done, ref} ->
              send(test, {ref, count})
          end
        end

        receive_calls.(receive_calls, 0)
      end)

    :erlang.trace_pattern(mfa, true, [])
    :erlang.trace(self(), true, [:call, :set_on_spawn, {:tracer, tracer}])

    result =
      try do
        fun.()
      after
        :erlang.trace(self(), false, [:call, :set_on_spawn])
        :erlang.trace_pattern(mfa, false, [])
      end

    # Trace messages come from other processes: make sure they have all reached the tracer.
    trace_ref = :erlang.trace_delivered(:all)
    assert_receive {:trace_delivered, :all, ^trace_ref}, 5_000

    ref = make_ref()
    send(tracer, {:done, ref})

    receive do
      {^ref, count} -> {result, count}
    end
  end
end
