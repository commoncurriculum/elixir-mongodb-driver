defmodule Mongo.StreamTest do
  use CollectionCase

  setup %{pid: top, test: test} do
    # A collection per test: some tests count the commands sent on it.
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

  # Command events carry decoded replies, so with a subscriber batches are decoded as before.
  defp without_command_subscribers(catcher) do
    ref = Process.monitor(catcher)
    EventCatcher.stop(catcher)
    assert_receive {:DOWN, ^ref, _, _, _}
    assert [] == Registry.lookup(:events_registry, :commands)
  end

  test "without command event subscribers, getMore batches are decoded as they are consumed", %{pid: top, coll: coll, catcher: catcher} do
    without_command_subscribers(catcher)
    %Mongo.Stream{session: {:own, session}, cursor: cursor_id} = stream = Mongo.find(top, coll, %{}, sort: %{index: 1}, batch_size: 300)

    get_more = [getMore: %BSON.LongNumber{value: cursor_id}, collection: coll, batchSize: 300]
    assert {:ok, %{"cursor" => %{"nextBatch" => %BSON.LazyDocuments{}}}} = Mongo.exec_command_session(session, get_more, Keyword.put(stream.opts, :lazy_cursor, true))
    assert {:ok, %{"cursor" => %{"nextBatch" => [_ | _]}}} = Mongo.exec_command_session(session, get_more, stream.opts)

    # Every document but the 600 read above.
    assert Enum.map(stream, & &1["index"]) == Enum.to_list(1..300) ++ Enum.to_list(901..1_000)
  end

  test "without command event subscribers, streams return every document in order", %{pid: top, coll: coll, catcher: catcher} do
    without_command_subscribers(catcher)

    for batch_size <- [1, 7, 100, 1_000] do
      assert Enum.to_list(1..1_000) == top |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: batch_size) |> Enum.map(& &1["index"])
    end

    assert [1, 2, 3] == top |> Mongo.find(coll, %{}, sort: %{index: 1}, batch_size: 100) |> Stream.drop(100) |> Stream.map(&(&1["index"] - 100)) |> Enum.take(3)
  end

  defmodule MatchStage do
    # A pipeline stage built by a query builder.
    defstruct [:filter]

    defimpl Mongo.Encoder do
      def encode(%{filter: filter}), do: %{"$match" => filter}
    end
  end

  test "aggregate pipelines whose stages are structs", %{pid: top, coll: coll, catcher: catcher} do
    without_command_subscribers(catcher)
    assert 1_000 == top |> Mongo.aggregate(coll, [%MatchStage{filter: %{}}], batch_size: 100) |> Enum.count()
  end

  defmodule FailingDecoder do
    def document(_binary), do: raise(ArgumentError, "bad document")
  end

  test "a batch that fails to decode raises the decoder's error and closes the cursor", %{pid: top, coll: coll, catcher: catcher} do
    stream = Mongo.find(top, coll, %{}, batch_size: 10)
    stream = %{stream | docs: %BSON.LazyDocuments{binary: <<0x03, "0", 0, 5, 0, 0, 0, 0>>, decoder: FailingDecoder}}

    assert_raise ArgumentError, "bad document", fn -> Enum.to_list(stream) end
    assert catcher |> EventCatcher.succeeded_events() |> Enum.any?(&(&1.command_name == :killCursors))
  end
end
