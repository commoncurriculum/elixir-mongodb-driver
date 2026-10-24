defmodule Mongo.Stream.Prefetch do
  @moduledoc false
  # Runs a function in another process and keeps its result until it is asked for.
  #
  # Mongo.Stream uses it to request the next getMore batch while the current one is consumed.
  # The result is not sent to the caller unasked: a stream suspended between demands (for
  # example in GenStage.from_enumerable/2) runs in a process whose receive loop could consume
  # it. The process is not linked; it exits when the process that started it exits (once `fun`
  # returns), or when the result is not asked for within `@hold_timeout`.

  # By default a server closes a cursor that has been idle this long (cursorTimeoutMillis),
  # so the stream could not have carried on after it anyway. Mongo.Stream does not prefetch
  # for cursors created with noCursorTimeout.
  @hold_timeout 10 * 60 * 1_000

  @spec start((-> term), timeout) :: pid
  def start(fun, hold_timeout \\ @hold_timeout) do
    owner = self()
    metadata = Logger.metadata()
    callers = [owner | Process.get(:"$callers", [])]

    spawn(fn ->
      owner_ref = Process.monitor(owner)
      Logger.metadata(metadata)
      Process.put(:"$callers", callers)

      result =
        try do
          {:ok, fun.()}
        catch
          kind, reason -> {:raised, kind, reason, __STACKTRACE__}
        end

      receive do
        {:get, ref, from} -> send(from, {ref, result})
        {:DOWN, ^owner_ref, :process, _pid, _reason} -> :ok
      after
        hold_timeout -> :ok
      end
    end)
  end

  @doc """
  Waits for the result: `{:ok, result}`, `{:raised, kind, reason, stacktrace}` if the function
  raised, or `{:exit, reason}` if the process is gone, for example because its result was
  already taken.
  """
  @spec await(pid) :: {:ok, term} | {:raised, :error | :exit | :throw, term, Exception.stacktrace()} | {:exit, term}
  def await(pid) do
    ref = Process.monitor(pid)
    send(pid, {:get, ref, self()})

    receive do
      {^ref, result} ->
        Process.demonitor(ref, [:flush])
        result

      {:DOWN, ^ref, :process, _pid, reason} ->
        {:exit, reason}
    end
  end
end
