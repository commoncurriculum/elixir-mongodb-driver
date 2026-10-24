defmodule Mongo.Stream.PrefetchTest do
  use ExUnit.Case, async: true

  alias Mongo.Stream.Prefetch

  test "await/1 returns the result, once" do
    pid = Prefetch.start(fn -> :result end)

    assert {:ok, :result} = Prefetch.await(pid)
    assert {:exit, _reason} = Prefetch.await(pid)
    assert {:messages, []} = Process.info(self(), :messages)
  end

  test "await/1 returns what the function raised, threw or exited with" do
    assert {:raised, :error, %ArgumentError{message: "boom"}, [_ | _]} = Prefetch.await(Prefetch.start(fn -> raise ArgumentError, "boom" end))
    assert {:raised, :throw, :ball, _stacktrace} = Prefetch.await(Prefetch.start(fn -> throw(:ball) end))
    assert {:raised, :exit, :bye, _stacktrace} = Prefetch.await(Prefetch.start(fn -> exit(:bye) end))
  end

  test "the result is kept until it is asked for, and not sent unasked" do
    pid = Prefetch.start(fn -> :result end)
    Process.sleep(50)

    assert Process.alive?(pid)
    assert {:messages, []} = Process.info(self(), :messages)
    assert {:ok, :result} = Prefetch.await(pid)
  end

  test "carries Logger metadata and $callers over" do
    Logger.metadata(request_id: "r1")
    pid = Prefetch.start(fn -> {Logger.metadata()[:request_id], Process.get(:"$callers")} end)

    assert {:ok, {"r1", [caller | _]}} = Prefetch.await(pid)
    assert caller == self()
  end

  test "exits with the process that started it, once the function returns" do
    test = self()

    owner =
      spawn(fn ->
        send(test, {:prefetch, Prefetch.start(fn -> receive(do: (:go -> :result)) end)})
        receive(do: (:stop -> :ok))
      end)

    assert_receive {:prefetch, pid}
    ref = Process.monitor(pid)
    send(owner, :stop)
    refute_receive {:DOWN, ^ref, _, _, _}, 50

    send(pid, :go)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
  end

  test "drops a result nobody asks for within the hold timeout" do
    pid = Prefetch.start(fn -> :result end, 50)
    ref = Process.monitor(pid)

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    assert {:exit, :noproc} = Prefetch.await(pid)
  end
end
