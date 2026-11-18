defmodule Mongo.App do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {Mongo.IdServer, []},
      {Mongo.PBKDF2Cache, []},
      %{
        id: Registry,
        start: {Registry, :start_link, [:duplicate, :events_registry]},
        type: :supervisor
      },
      Supervisor.child_spec({Registry, keys: :unique, name: Mongo.Telemetry}, id: Mongo.Telemetry)
    ]

    opts = [strategy: :one_for_one, name: Mongo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
