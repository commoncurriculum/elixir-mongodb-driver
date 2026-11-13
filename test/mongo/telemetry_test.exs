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

  test "the execution event carries the write errors of a write that failed", %{pid: top, coll: coll} do
    {:error, error} = Mongo.update_one(top, coll, %{}, %{"$bad" => %{name: "b"}})
    assert_receive {:executed, %{command: :update, collection: ^coll, error: ^error}}
    assert [%{"code" => 9}] = error.write_errors
  end
end
