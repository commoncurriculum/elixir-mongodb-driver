defmodule Mongo.TelemetryTest do
  use CollectionCase

  def forward(_event, _measurements, metadata, parent), do: send(parent, {:executed, metadata})

  setup %{test: test} do
    :telemetry.attach("#{inspect(test)}", [:mongodb_driver, :execution], &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach("#{inspect(test)}") end)
    [coll: "telemetry_#{:erlang.phash2(test)}"]
  end

  test "the execution event names the database", %{pid: top, coll: coll} do
    {:ok, _} = Mongo.insert_one(top, coll, %{name: "a"})
    assert_receive {:executed, %{command: :insert, collection: ^coll, database: "mongodb_test", error: nil}}
  end

  test "the execution event of a command that names no collection has none", %{pid: top} do
    {:ok, _} = Mongo.command(top, ping: 1)
    assert_receive {:executed, %{command: :ping, collection: nil}}
  end

  test "the execution event of a getMore names its collection", %{pid: top, coll: coll} do
    {:ok, _} = Mongo.insert_many(top, coll, [%{n: 1}, %{n: 2}, %{n: 3}])
    [_, _, _] = top |> Mongo.find(coll, %{}, batch_size: 1) |> Enum.to_list()
    assert_receive {:executed, %{command: :getMore, collection: ^coll}}
  end

  test "the execution event carries the error of a command that failed", %{pid: top, coll: coll} do
    {:error, error} = Mongo.find_one(top, coll, %{"$bad" => 1})
    assert_receive {:executed, %{command: :find, collection: ^coll, database: "mongodb_test", error: ^error}}
    assert error.code == 2
  end

  test "only the client pool carries the topology's label, not the monitors' connections" do
    test = self()
    id = make_ref()

    :telemetry.attach(id, [:db_connection, :checkout], fn _, _, metadata, _ -> send(test, {:checkout, metadata}) end, nil)
    on_exit(fn -> :telemetry.detach(id) end)

    {:ok, top} = Mongo.start_link(database: "mongodb_test", seeds: ["127.0.0.1:27017"], label: :labeled_topology)
    {:ok, _} = Mongo.command(top, ping: 1)
    Process.sleep(200)

    {labeled, unlabeled} =
      Stream.repeatedly(fn ->
        receive do
          {:checkout, metadata} -> metadata
        after
          0 -> nil
        end
      end)
      |> Enum.take_while(& &1)
      |> Enum.split_with(&(&1.label == :labeled_topology))

    assert [_ | _] = labeled
    assert [_ | _] = unlabeled
    assert [_client_pool] = labeled |> Enum.map(& &1.pool) |> Enum.uniq()
  end

  test "the execution event carries the write errors of a write that failed", %{pid: top, coll: coll} do
    {:error, error} = Mongo.update_one(top, coll, %{}, %{"$bad" => %{name: "b"}})
    assert_receive {:executed, %{command: :update, collection: ^coll, error: ^error}}
    assert [%{"code" => 9}] = error.write_errors
  end

  def forward_command(event, measurements, metadata, parent), do: send(parent, {:command, List.last(event), measurements, metadata})

  defp attach_commands(test) do
    id = make_ref()
    events = [[:mongodb_driver, :command, :start], [:mongodb_driver, :command, :stop], [:mongodb_driver, :command, :exception]]
    :telemetry.attach_many(id, events, &__MODULE__.forward_command/4, test)
    on_exit(fn -> :telemetry.detach(id) end)
  end

  test "a command emits start and stop in the calling process, with its database", %{pid: top, coll: coll} do
    attach_commands(self())
    {:ok, _} = Mongo.insert_one(top, coll, %{name: "a"})

    assert_receive {:command, :start, %{system_time: _}, %{command: :insert, collection: ^coll, caller: caller, telemetry_span_context: ref}}
    assert caller == self()
    assert_receive {:command, :stop, %{duration: duration}, %{command: :insert, collection: ^coll, database: "mongodb_test", address: address, error: nil, telemetry_span_context: ^ref}}
    assert address =~ ":27017"
    assert duration > 0
  end

  test "a command names a collection only when it acts on one", %{pid: top, coll: coll} do
    attach_commands(self())
    {:ok, _} = Mongo.insert_one(top, coll, %{name: "a"})
    {:ok, _} = Mongo.admin_command(top, renameCollection: "mongodb_test.#{coll}", to: "mongodb_test.#{coll}_renamed")
    {:ok, _} = Mongo.admin_command(top, getParameter: "*")
    renamed = "#{coll}_renamed"

    assert_receive {:command, :start, _, %{command: :renameCollection, collection: ^coll}}
    assert_receive {:command, :start, _, %{command: :getParameter, collection: nil}}
    {:ok, _} = Mongo.command(top, drop: renamed)
  end

  test "a command that fails on the server stops with its error", %{pid: top, coll: coll} do
    attach_commands(self())
    {:error, error} = Mongo.find_one(top, coll, %{"$bad" => 1})
    assert_receive {:command, :stop, _, %{command: :find, error: ^error}}
  end

  test "a command whose connection times out stops with the network error", %{pid: top, coll: coll} do
    {:ok, _} = Mongo.insert_one(top, coll, %{name: "a"})
    attach_commands(self())

    {:error, %Mongo.Error{message: message} = error} = Mongo.find_one(top, coll, %{"$where" => "sleep(500) || true"}, timeout: 100)

    assert message =~ "tcp recv"
    assert_receive {:command, :stop, _, %{command: :find, collection: ^coll, address: address, error: ^error}}
    assert address =~ ":27017"
  end

  test "a getMore stops when its reply arrives, before its batch is consumed", %{pid: top, coll: coll} do
    {:ok, _} = Mongo.insert_many(top, coll, Enum.map(1..300, &%{n: &1}))
    attach_commands(self())

    top
    |> Mongo.find(coll, %{}, batch_size: 150)
    |> Enum.each(fn _doc -> :ok end)

    assert_receive {:command, :stop, _, %{command: :getMore, collection: ^coll, database: "mongodb_test"}}
  end

  test "a write concern error fails the command", %{pid: top, coll: coll} do
    attach_commands(self())
    {:ok, _} = Mongo.insert_one(top, coll, %{name: "a"}, w: 2, wtimeout: 1)
    assert_receive {:command, :stop, _, %{command: :insert, error: %Mongo.Error{}}}
    assert_receive {:executed, %{command: :insert, error: %Mongo.Error{}}}
  end

  test "a transaction's commit names the process that committed it", %{pid: top, coll: coll} do
    attach_commands(self())
    test = self()

    :ok =
      Mongo.transaction(top, fn ->
        {:ok, _} = Mongo.insert_one(top, coll, %{name: "a"})
        :ok
      end)

    assert_receive {:command, :start, _, %{command: :commitTransaction, caller: ^test}}
    assert_receive {:command, :stop, _, %{command: :commitTransaction, error: nil}}
  end
end
