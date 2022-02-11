defmodule RaxElasticlunr do
  alias Elasticlunr.{Field, Index, Pipeline}
  @type index_name :: atom()
  @type document :: %{String.t() => Jason.Encoder.t()}

  # API

  @spec create_index(Rax.Cluster.name(), index_name(), Pipeline.t()) :: :ok
  def create_index(cluster, index_name, %Pipeline{} = pipeline) do
    Rax.call(cluster, {:create_index, index_name, pipeline})
    |> handle_call()
  end

  @spec drop_index(Rax.Cluster.name(), index_name()) :: :ok | nil
  def drop_index(cluster, index_name) do
    Rax.call(cluster, {:drop_index, index_name})
    |> handle_call()
  end

  @spec add_field(Rax.Cluster.name(), index_name(), Index.document_field(), keyword()) :: :ok
  def add_field(cluster, index_name, field, opts \\ []) do
    Rax.call(cluster, {:add_field, index_name, field, opts})
    |> handle_call()
  end

  @spec update_field(Rax.Cluster.name(), index_name(), Index.document_field(), Field.t()) :: :ok
  def update_field(cluster, index_name, field, %Field{} = opts) do
    Rax.call(cluster, {:update_field, index_name, field, opts})
    |> handle_call()
  end

  @spec get_field(Rax.Cluster.name(), index_name(), Index.document_field()) :: Field.t() | nil
  def get_field(cluster, index_name, field) do
    Rax.query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          Index.get_field(index, field)

        :error ->
          :error
      end
    end)
    |> case do
      :error ->
        raise "Elasticlunr index does not exist"

      resp ->
        resp
    end
  end

  @spec get_fields(Rax.Cluster.name(), index_name()) :: :ok | {:error, atom()}
  def get_fields(cluster, index_name) do
    Rax.query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          Index.get_fields(index)

        :error ->
          :error
      end
    end)
    |> case do
      :error ->
        raise "Elasticlunr index does not exist"

      resp ->
        resp
    end
  end

  @spec add_documents(Rax.Cluster.name(), index_name(), [document()]) :: :ok
  def add_documents(cluster, index_name, documents) do
    Rax.call(cluster, {:add_documents, index_name, documents})
    |> handle_call()
  end

  @spec update_documents(Rax.Cluster.name(), index_name(), [document()]) :: :ok
  def update_documents(cluster, index_name, documents) do
    Rax.call(cluster, {:update_documents, index_name, documents})
    |> handle_call()
  end

  @spec remove_documents(Rax.Cluster.name(), index_name(), [Jason.Encoder.t()]) :: :ok
  def remove_documents(cluster, index_name, document_ids) do
    Rax.call(cluster, {:remove_documents, index_name, document_ids})
    |> handle_call()
  end

  @spec analyze(Rax.Cluster.name(), index_name(), Index.document_field(), any(), keyword()) :: Token.t() | list(Token.t())
  def analyze(cluster, index_name, field, content, opts) do
    Rax.query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          Index.analyze(index, field, content, opts)

        :error ->
          :error
      end
    end)
    |> case do
      :error ->
        raise "Elasticlunr index does not exist"

      resp ->
        resp
    end
  end

  @spec terms(Rax.Cluster.name(), index_name(), keyword()) :: any()
  def terms(cluster, index_name, query) do
    Rax.query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          {:ok, Index.terms(index, query)}

        :error ->
          :error
      end
    end)
    |> case do
      :error ->
        raise "Elasticlunr index does not exist"

      {:ok, resp} ->
        resp
    end
  end

  @spec all(Rax.Cluster.name(), index_name()) :: list(Field.document_ref())
  def all(cluster, index_name) do
    Rax.query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          Index.all(index)

        :error ->
          :error
      end
    end)
    |> case do
      :error ->
        raise "Elasticlunr index does not exist"

      resp ->
        resp
    end
  end

  @spec search(Rax.Cluster.name(), index_name(), Index.search_query(), map() | nil) :: [Index.search_result()]
  def search(cluster, index_name, query, opts \\ nil) do
    Rax.query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          Index.search(index, query, opts)

        :error ->
          :error
      end
    end)
    |> case do
      :error ->
        raise "Elasticlunr index does not exist"

      resp ->
        resp
    end
  end

  @spec local_search(Rax.Cluster.name(), index_name(), Index.search_query(), map() | nil) :: [Index.search_result()]
  def local_search(cluster, index_name, query, opts \\ nil) do
    Rax.local_query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          Index.search(index, query, opts)

        :error ->
          :error
      end
    end)
    |> case do
      :error ->
        raise "Elasticlunr index does not exist"

      resp ->
        resp
    end
  end


  # State machine

  use Rax.Machine
  @doc false
  def init(_config) do
    %{indices: %{}}
  end

  @doc false
  def apply(_meta, {:create_index, index_name, pipeline}, state) do
    if Map.has_key?(state.indices, index_name) do
      {state, {:error, :existing_index}}
    else
      index = Index.new(pipeline: pipeline)
      {put_in(state.indices[index_name], index), :ok}
    end
  end

  def apply(_meta, {:drop_index, index_name}, state) do
    if Map.has_key?(state.indices, index_name) do
      {update_in(state.indices, &Map.delete(&1, index_name)), :ok}
    else
      {state, nil}
    end
  end

  def apply(_meta, {:add_field, index_name, field, opts}, state) do
    case Map.fetch(state.indices, index_name) do
      {:ok, index} ->
        index = Index.add_field(index, field, opts)
        {put_in(state.indices[index_name], index), :ok}

      :error ->
        {state, {:error, :index_not_found}}
    end
  end

  def apply(_meta, {:update_field, index_name, field, opts}, state) do
    case Map.fetch(state.indices, index_name) do
      {:ok, index} ->
        index = Index.update_field(index, field, opts)
        {put_in(state.indices[index_name], index), :ok}

      :error ->
        {state, {:error, :index_not_found}}
    end
  end

  def apply(_meta, {:add_documents, index_name, documents}, state) do
    case Map.fetch(state.indices, index_name) do
      {:ok, index} ->
        index = Index.add_documents(index, documents)
        {put_in(state.indices[index_name], index), :ok}

      :error ->
        {state, {:error, :index_not_found}}
    end
  end

  def apply(_meta, {:update_documents, index_name, documents}, state) do
    case Map.fetch(state.indices, index_name) do
      {:ok, index} ->
        index = Index.update_documents(index, documents)
        {put_in(state.indices[index_name], index), :ok}

      :error ->
        {state, {:error, :index_not_found}}
    end
  end

  def apply(_meta, {:remove_documents, index_name, document_ids}, state) do
    case Map.fetch(state.indices, index_name) do
      {:ok, index} ->
        index = Index.remove_documents(index, document_ids)
        {put_in(state.indices[index_name], index), :ok}

      :error ->
        {state, {:error, :index_not_found}}
    end
  end

  def apply(meta, cmd, state) do
    super(meta, cmd, state)
  end

  defp handle_call({:error, e}) do
    raise "RaxElasticlunr error: #{e}"
  end
  defp handle_call(resp), do: resp
end
