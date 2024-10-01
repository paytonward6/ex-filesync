
defmodule FileSync.Worker do
  require Logger
  use GenServer

  def init(%{}) do
    root_dir = root_dir()
    File.mkdir_p!(root_dir)
    Logger.info("Write directory found: #{root_dir}")
    {:ok, %{ root_dir: root_dir }}
  end

  def start_link(%{} = arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @spec sync(node() | [node()] , Path.t()) :: :ok
  def sync(server \\ Node.list(), path)

  def sync(server, path) when is_atom(server) and server != node() do
    GenServer.call(__MODULE__, {:sync, [server], path})
  end

  def sync(servers, path) when is_list(servers) do
    GenServer.call(__MODULE__, {:sync, servers, path})
  end

  def handle_call({:sync, servers, path}, _from, %{root_dir: dir} = state) do

    {replies, bad_nodes} = GenServer.multi_call(servers, __MODULE__, {:get, path})

    source =
      Path.join(dir, path)
      |> File.stream!()

    if not File.exists?(source.path) do
      raise "File #{source.path} DNE"
    end

    replies
    |> Enum.map(&(Task.async(fn -> remote_write(&1, source) end)))
    |> Task.await_many()

    Logger.info("Transferred file '#{source.path}' to: #{replies |> reply_to_string}")
    {:reply, :ok, state}
  end

  def handle_call({:get, path}, _from, %{root_dir: dir} = state) do
    file =
      Path.join(dir, path)
      |> File.stream!()
    {:reply, file, state}
  end

  defp remote_write({_from, dest}, %File.Stream{} = file_stream) do
    file_stream
    |> Stream.into(dest)
    |> Stream.run()
  end

  def root_dir() do
    case System.fetch_env("ROOT_DIR") do
      {:ok, dir} -> dir
      :error -> System.tmp_dir!() |> Path.join(Node.self() |> to_string)
    end
  end

  def reply_to_string([]), do: "[]"
  def reply_to_string(replies) when is_list(replies) do
    replies |> Keyword.keys |> Enum.join(", ")
  end
end

