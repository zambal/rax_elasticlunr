defmodule RaxElasticlunr do
  alias Elasticlunr.{Field, Index, Pipeline}
  @type index_name :: any()
  @type document :: %{String.t() => Jason.Encoder.t()}

  # API

  @spec create_index(Rax.Cluster.name(), index_name(), Pipeline.t()) :: :ok
  def create_index(cluster, index_name, %Pipeline{} = pipeline) do
    Rax.call(cluster, {:create_index, index_name, pipeline})
    |> handle_response()
  end

  @spec drop_index(Rax.Cluster.name(), index_name()) :: :ok | nil
  def drop_index(cluster, index_name) do
    Rax.call(cluster, {:drop_index, index_name})
    |> handle_response()
  end

  @spec add_field(Rax.Cluster.name(), index_name(), Index.document_field(), keyword()) :: :ok
  def add_field(cluster, index_name, field, opts \\ []) do
    Rax.call(cluster, {:add_field, index_name, field, opts})
    |> handle_response()
  end

  @spec update_field(Rax.Cluster.name(), index_name(), Index.document_field(), Field.t()) :: :ok
  def update_field(cluster, index_name, field, %Field{} = opts) do
    Rax.call(cluster, {:update_field, index_name, field, opts})
    |> handle_response()
  end

  @spec get_field(Rax.Cluster.name(), index_name(), Index.document_field()) :: Field.t() | nil
  def get_field(cluster, index_name, field) do
    handle_query(cluster, index_name, &Index.get_field(&1, field))
  end

  @spec get_fields(Rax.Cluster.name(), index_name()) :: :ok | {:error, atom()}
  def get_fields(cluster, index_name) do
    handle_query(cluster, index_name, &Index.get_fields/1)
  end

  @spec add_documents(Rax.Cluster.name(), index_name(), [document()]) :: :ok
  def add_documents(cluster, index_name, documents) do
    Rax.call(cluster, {:add_documents, index_name, documents})
    |> handle_response()
  end

  @spec update_documents(Rax.Cluster.name(), index_name(), [document()]) :: :ok
  def update_documents(cluster, index_name, documents) do
    Rax.call(cluster, {:update_documents, index_name, documents})
    |> handle_response()
  end

  @spec remove_documents(Rax.Cluster.name(), index_name(), [Jason.Encoder.t()]) :: :ok
  def remove_documents(cluster, index_name, document_ids) do
    Rax.call(cluster, {:remove_documents, index_name, document_ids})
    |> handle_response()
  end

  @spec analyze(Rax.Cluster.name(), index_name(), Index.document_field(), any(), keyword()) :: Token.t() | list(Token.t())
  def analyze(cluster, index_name, field, content, opts) do
    handle_query(cluster, index_name, &Index.analyze(&1, field, content, opts))
  end

  @spec terms(Rax.Cluster.name(), index_name(), keyword()) :: any()
  def terms(cluster, index_name, query) do
    handle_query(cluster, index_name, &Index.terms(&1, query))
  end

  @spec all(Rax.Cluster.name(), index_name()) :: list(Field.document_ref())
  def all(cluster, index_name) do
    handle_query(cluster, index_name, &Index.all/1)
  end

  @spec search(Rax.Cluster.name(), index_name(), Index.search_query(), map() | nil) :: [Index.search_result()]
  def search(cluster, index_name, query, opts \\ nil) do
    handle_query(cluster, index_name, &Index.search(&1, query, opts))
  end

  @spec local_search(Rax.Cluster.name(), index_name(), Index.search_query(), map() | nil) :: [Index.search_result()]
  def local_search(cluster, index_name, query, opts \\ nil) do
    handle_local_query(cluster, index_name, &Index.search(&1, query, opts))
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
    handle_apply(index_name, state, &Index.add_field(&1, field, opts))
  end

  def apply(_meta, {:update_field, index_name, field, opts}, state) do
    handle_apply(index_name, state, &Index.update_field(&1, field, opts))
  end

  def apply(_meta, {:add_documents, index_name, documents}, state) do
    handle_apply(index_name, state, &Index.add_documents(&1, documents))
  end

  def apply(_meta, {:update_documents, index_name, documents}, state) do
    handle_apply(index_name, state, &Index.update_documents(&1, documents))
  end

  def apply(_meta, {:remove_documents, index_name, document_ids}, state) do
    handle_apply(index_name, state, &Index.remove_documents(&1, document_ids))
  end

  def apply(meta, cmd, state) do
    super(meta, cmd, state)
  end

  defp handle_apply(index_name, state, fun) do
    case Map.fetch(state.indices, index_name) do
      {:ok, index} ->
        index = fun.(index)
        {put_in(state.indices[index_name], index), :ok}

      :error ->
        {state, :error}
    end
  end

  defp handle_query(cluster, index_name, fun) do
    Rax.query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          {:ok, fun.(index)}

        :error ->
          :error
      end
    end)
    |> handle_response()
  end

  defp handle_local_query(cluster, index_name, fun) do
    Rax.local_query(cluster, fn state ->
      case Map.fetch(state.indices, index_name) do
        {:ok, index} ->
          {:ok, fun.(index)}

        :error ->
          :error
      end
    end)
    |> handle_response()
  end

  defp handle_response(:error) do
    raise "Elasticlunr index does not exist"
  end
  defp handle_response({:ok, resp}), do: resp
  defp handle_response(:ok), do: :ok
  defp handle_response(nil), do: nil
end
